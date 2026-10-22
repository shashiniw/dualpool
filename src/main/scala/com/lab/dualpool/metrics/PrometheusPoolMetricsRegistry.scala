package com.lab.dualpool.metrics

import com.lab.dualpool.DatabaseRole
import com.zaxxer.hikari.metrics.MetricsTrackerFactory
import com.zaxxer.hikari.metrics.prometheus.PrometheusMetricsTrackerFactory
import io.prometheus.client.{Collector, CollectorRegistry, CounterMetricFamily, GaugeMetricFamily}
import org.slf4j.LoggerFactory

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

/**
 * Exports every pool into a Prometheus [[CollectorRegistry]], each series carrying a `pool` label whose value is
 * the pool name (proposal §5.5 "Per-pool naming").
 *
 * The `hikaricp_*` families come from HikariCP's own exporter, which the library attaches to each pool through
 * [[metricsTrackerFactory]]. Only the Slick executor families are the library's own, because the executor queue
 * is the metric Slick does not publish and the one §5.5 says to alert on.
 *
 * Obtain one with [[PrometheusPoolMetricsRegistry.forRegistry]]: a Prometheus registry accepts a given collector
 * only once, so a second instance pointed at the same registry would register nothing and report every pool as
 * missing its metrics — the §5.5 failure where absent metrics read as healthy.
 *
 * @param registry defaults to the default registry, which is the one a service's `/metrics` endpoint scrapes
 */
final class PrometheusPoolMetricsRegistry private (registry: CollectorRegistry) extends PoolMetricsRegistry {

  import PrometheusPoolMetricsRegistry._

  private val sources = new ConcurrentHashMap[DatabaseRole, Registration]()
  private val collector = new SlickExecutorCollector(() => sources.values().asScala.toVector.map(_.stats))
  @volatile private var collectorRegistered = false

  // One factory per registry. HikariCP keeps the pools it has been told about in a single collector, so both
  // pools land in the same families, distinguished by the `pool` label rather than by metric name.
  // Wrapped so acquire time is also recorded as a histogram, which is the only form that aggregates across pods.
  private val acquireHistogram = ConnectionAcquireHistogram.register(registry)
  private val trackerFactory =
    new DualPoolMetricsTrackerFactory(new PrometheusMetricsTrackerFactory(registry), acquireHistogram)

  def metricsTrackerFactory: Option[MetricsTrackerFactory] = Some(trackerFactory)

  def register(role: DatabaseRole, stats: PoolStatsSource, strict: Boolean): Unit = synchronized {
    sources.put(role, Registration(stats, strict))
    // Create the child now so the pool's buckets exist before it has served a query (§5.5 registration timing).
    acquireHistogram.labels(stats.poolName)
    if (!collectorRegistered) {
      registry.register(collector)
      collectorRegistered = true
    }
  }

  /**
   * Proves every pool publishes every name in [[PoolMetrics.MetricNames]] under its own `pool` label.
   *
   * This is the §5.5 "registration timing" check. HikariCP creates its Summary and Counter children when the
   * pool is created, and the library forces the Slick executor MBean at construction, so all seven families
   * exist before the pool has served a single query.
   *
   * A pool the library did not build is reported but not fatal: HikariCP only registers a pool with its
   * exporter once that pool actually starts, and when the service owns the pool the library cannot insist on
   * that having happened yet.
   */
  def verifyRegistered(): Unit = {
    val published = publishedNamesByPool()

    def missingFor(registration: Registration, role: DatabaseRole): Seq[String] = {
      val names = published.getOrElse(registration.stats.poolName, Set.empty[String])
      PoolMetrics.MetricNames
        .filterNot(required => names.exists(_.startsWith(required)))
        .map(required => s"pool '${registration.stats.poolName}' ($role) is missing metric '$required'")
    }

    val (strict, lenient) = sources.asScala.toSeq.partition(_._2.strict)

    val lenientMissing = lenient.flatMap { case (role, registration) => missingFor(registration, role) }
    if (lenientMissing.nonEmpty) {
      logger.warn("dualpool: metrics for a pool the service owns are not published yet; they will appear once " +
        "that pool starts:{}", lenientMissing.mkString("\n  - ", "\n  - ", ""))
    }

    val strictMissing = strict.flatMap { case (role, registration) => missingFor(registration, role) }
    if (strictMissing.nonEmpty) {
      throw new IllegalStateException(
        strictMissing.mkString("dualpool: pool metrics are not fully registered:\n  - ", "\n  - ", ""))
    }
  }

  def snapshot(role: DatabaseRole): PoolMetrics = {
    val registration = sources.get(role)
    if (registration == null) throw new IllegalArgumentException(s"dualpool: no pool registered for role $role")
    PoolMetrics.from(registration.stats, connectionWaitMillisP95(registration.stats.poolName))
  }

  def snapshotAll(): Map[DatabaseRole, PoolMetrics] =
    sources.asScala.map { case (role, registration) =>
      role -> PoolMetrics.from(registration.stats, connectionWaitMillisP95(registration.stats.poolName))
    }.toMap

  def unregister(role: DatabaseRole): Unit = { sources.remove(role); () }

  /** The 0.95 quantile of `hikaricp_connection_acquired_nanos`, in millis, or -1 when nothing has been observed. */
  private def connectionWaitMillisP95(poolName: String): Long =
    findSample("hikaricp_connection_acquired_nanos", Map("pool" -> poolName, "quantile" -> "0.95"))
      .filterNot(_.isNaN)
      .map(nanos => (nanos / 1000000.0).round)
      .getOrElse(PoolMetrics.Unavailable.toLong)

  private def findSample(name: String, labels: Map[String, String]): Option[Double] =
    registry.metricFamilySamples().asScala
      .flatMap(_.samples.asScala)
      .find { sample =>
        sample.name == name && labels.forall { case (labelName, labelValue) =>
          val index = sample.labelNames.indexOf(labelName)
          index >= 0 && sample.labelValues.get(index) == labelValue
        }
      }
      .map(_.value)

  /** Every metric name currently published, grouped by the value of its `pool` label. */
  private def publishedNamesByPool(): Map[String, Set[String]] =
    registry.metricFamilySamples().asScala
      .flatMap(_.samples.asScala)
      .flatMap { sample =>
        val index = sample.labelNames.indexOf("pool")
        if (index >= 0) Some(sample.labelValues.get(index) -> sample.name) else None
      }
      .toSeq
      .groupBy(_._1)
      .map { case (pool, pairs) => pool -> pairs.map(_._2).toSet }
}

object PrometheusPoolMetricsRegistry {

  /** A registered pool and whether its metrics are allowed to fail the service start. */
  private final case class Registration(stats: PoolStatsSource, strict: Boolean)

  private val logger = LoggerFactory.getLogger(classOf[PrometheusPoolMetricsRegistry])

  private val instances = new ConcurrentHashMap[CollectorRegistry, PrometheusPoolMetricsRegistry]()

  /**
   * The one instance for this Prometheus registry, created on first use.
   *
   * Both collectors behind these metrics — HikariCP's and the library's — can be registered into a given
   * [[CollectorRegistry]] exactly once. Sharing the instance is what lets a service build a `DualPool` more than
   * once in a JVM (a restart, or a test suite) without the second one reporting nothing.
   */
  def forRegistry(registry: CollectorRegistry = CollectorRegistry.defaultRegistry): PrometheusPoolMetricsRegistry =
    instances.computeIfAbsent(registry, r => new PrometheusPoolMetricsRegistry(r))

  /**
   * The executor queue, which Slick exposes over JMX but not to Prometheus.
   *
   * `slick_executor_queue_size` is the saturation signal to alert on: `hikaricp_pending_threads` can read zero
   * while this is deep, because work waits in the executor queue before it ever asks for a connection (§5.5).
   */
  private final class SlickExecutorCollector(sources: () => Seq[PoolStatsSource]) extends Collector {

    def collect(): java.util.List[Collector.MetricFamilySamples] = {
      val queueSize = new GaugeMetricFamily("slick_executor_queue_size",
        "Requests waiting in the Slick executor queue", List("pool").asJava)
      val queueCapacity = new GaugeMetricFamily("slick_executor_queue_capacity",
        "Bound on the Slick executor queue", List("pool").asJava)
      val activeThreads = new GaugeMetricFamily("slick_executor_active_threads",
        "Threads currently executing in the Slick executor", List("pool").asJava)
      val rejected = new CounterMetricFamily("slick_executor_rejected",
        "Work refused because the Slick executor queue was full", List("pool").asJava)

      sources().foreach { stats =>
        val labels = List(stats.poolName).asJava
        queueSize.addMetric(labels, stats.executorQueueDepth.toDouble)
        queueCapacity.addMetric(labels, stats.executorQueueCapacity.toDouble)
        activeThreads.addMetric(labels, stats.executorActiveThreads.toDouble)
        // Only executors the library built can be counted; no series rather than a counter stuck at -1.
        if (stats.executorRejected >= 0) rejected.addMetric(labels, stats.executorRejected.toDouble)
      }

      List[Collector.MetricFamilySamples](queueSize, queueCapacity, activeThreads, rejected).asJava
    }
  }
}
