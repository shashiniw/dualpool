package com.lab.dualpool.metrics

import com.lab.dualpool.DatabaseRole
import com.zaxxer.hikari.metrics.MetricsTrackerFactory
import com.zaxxer.hikari.metrics.prometheus.PrometheusMetricsTrackerFactory
import io.prometheus.client.CollectorRegistry
import org.slf4j.LoggerFactory

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

/**
 * Exports every pool into a Prometheus [[CollectorRegistry]], each series carrying a `pool` label whose value
 * is the pool name.
 *
 * The `hikaricp_*` families come from HikariCP's own exporter, which the library attaches to each pool through
 * [[metricsTrackerFactory]]. The `slick_executor_*` families are this library's, because the executor queue is
 * what HikariCP does not publish and what saturation shows up in first.
 *
 * Obtain one with [[PrometheusPoolMetricsRegistry.forRegistry]] rather than constructing it.
 */
final class PrometheusPoolMetricsRegistry private (registry: CollectorRegistry) extends PoolMetricsRegistry {

  import PrometheusPoolMetricsRegistry._

  private val sources = new ConcurrentHashMap[DatabaseRole, Registration]()
  private val collector = new SlickExecutorCollector(() => sources.values().asScala.toVector.map(_.stats))
  @volatile private var collectorRegistered = false

  // One factory per registry. HikariCP keeps the pools it has been told about in a single collector, so every
  // pool lands in the same families, distinguished by the `pool` label rather than by metric name. It is
  // wrapped so acquire time is also recorded as a histogram, which is the only form that aggregates over pods.
  private val acquireHistogram = ConnectionAcquireHistogram.register(registry)
  private val trackerFactory =
    new HistogramMetricsTrackerFactory(new PrometheusMetricsTrackerFactory(registry), acquireHistogram)

  def metricsTrackerFactory: Option[MetricsTrackerFactory] = Some(trackerFactory)

  def register(role: DatabaseRole, stats: PoolStatsSource, strict: Boolean): Unit = synchronized {
    sources.put(role, Registration(stats, strict))
    // Create the child now, so the pool's buckets exist before it has served a query.
    acquireHistogram.labels(stats.poolName)
    if (!collectorRegistered) {
      registry.register(collector)
      collectorRegistered = true
    }
  }

  /**
   * Proves every pool publishes every name in [[PoolMetrics.MetricNames]] under its own `pool` label.
   *
   * HikariCP creates its children when a pool is created and the library forces the Slick executor MBean at
   * construction, so all of the families exist before a pool has served a single query.
   *
   * A pool the library did not build is reported but not fatal: HikariCP registers a pool with its exporter
   * only once that pool starts, and the library cannot insist that a pool the service owns has started yet.
   */
  def verifyRegistered(): Unit = {
    val published = publishedNamesByPool()

    def missingFor(role: DatabaseRole, registration: Registration): Seq[String] = {
      val names = published.getOrElse(registration.stats.poolName, Set.empty[String])
      PoolMetrics.MetricNames
        .filterNot(required => names.exists(_.startsWith(required)))
        .map(required => s"pool '${registration.stats.poolName}' ($role) is missing metric '$required'")
    }

    val (strict, lenient) = sources.asScala.toSeq.partition { case (_, registration) => registration.strict }

    val lenientMissing = lenient.flatMap { case (role, registration) => missingFor(role, registration) }
    if (lenientMissing.nonEmpty) {
      logger.warn("dualpool: metrics for a pool the service owns are not published yet; they will appear once " +
        "that pool starts:{}", lenientMissing.mkString("\n  - ", "\n  - ", ""))
    }

    val strictMissing = strict.flatMap { case (role, registration) => missingFor(role, registration) }
    if (strictMissing.nonEmpty) {
      throw new IllegalStateException(
        strictMissing.mkString("dualpool: pool metrics are not fully registered:\n  - ", "\n  - ", ""))
    }
  }

  def snapshot(role: DatabaseRole): PoolMetrics =
    Option(sources.get(role))
      .map(registration => PoolMetrics.from(registration.stats, connectionWaitMillisP95(registration.stats.poolName)))
      .getOrElse(throw new IllegalArgumentException(s"dualpool: no pool registered for role $role"))

  def snapshotAll(): Map[DatabaseRole, PoolMetrics] =
    sources.asScala.map { case (role, registration) =>
      role -> PoolMetrics.from(registration.stats, connectionWaitMillisP95(registration.stats.poolName))
    }.toMap

  def unregister(role: DatabaseRole): Unit = {
    sources.remove(role)
    ()
  }

  /** The 0.95 quantile of `hikaricp_connection_acquired_nanos`, in millis, or -1 when nothing was observed. */
  private def connectionWaitMillisP95(poolName: String): Long =
    findSample("hikaricp_connection_acquired_nanos", Map("pool" -> poolName, "quantile" -> "0.95"))
      .filterNot(_.isNaN)
      .map(nanos => (nanos / 1000000.0).round)
      .getOrElse(PoolMetrics.Unavailable.toLong)

  private def findSample(name: String, labels: Map[String, String]): Option[Double] =
    samples()
      .find { sample =>
        sample.name == name && labels.forall { case (labelName, labelValue) =>
          labelOf(sample, labelName).contains(labelValue)
        }
      }
      .map(_.value)

  /** Every metric name currently published, grouped by the value of its `pool` label. */
  private def publishedNamesByPool(): Map[String, Set[String]] =
    samples()
      .flatMap(sample => labelOf(sample, "pool").map(_ -> sample.name))
      .toSeq
      .groupBy { case (pool, _) => pool }
      .map { case (pool, pairs) => pool -> pairs.map { case (_, name) => name }.toSet }

  private def samples(): Iterator[Sample] =
    registry.metricFamilySamples().asScala.flatMap(_.samples.asScala)

  private def labelOf(sample: Sample, labelName: String): Option[String] = {
    val index = sample.labelNames.indexOf(labelName)
    if (index >= 0) Some(sample.labelValues.get(index)) else None
  }
}

object PrometheusPoolMetricsRegistry {

  private type Sample = io.prometheus.client.Collector.MetricFamilySamples.Sample

  private val logger = LoggerFactory.getLogger(classOf[PrometheusPoolMetricsRegistry])

  private val instances = new ConcurrentHashMap[CollectorRegistry, PrometheusPoolMetricsRegistry]()

  /** A registered pool and whether its metrics are allowed to fail the service start. */
  private final case class Registration(stats: PoolStatsSource, strict: Boolean)

  /**
   * The one instance for this Prometheus registry, created on first use.
   *
   * Both collectors behind these metrics — HikariCP's and the library's — can be registered into a given
   * [[CollectorRegistry]] exactly once. Sharing the instance is what lets a service build a `DualPool` more
   * than once in a JVM (a restart, or a test suite) without the second one silently reporting nothing.
   */
  def forRegistry(registry: CollectorRegistry = CollectorRegistry.defaultRegistry): PrometheusPoolMetricsRegistry =
    instances.computeIfAbsent(registry, candidate => new PrometheusPoolMetricsRegistry(candidate))
}
