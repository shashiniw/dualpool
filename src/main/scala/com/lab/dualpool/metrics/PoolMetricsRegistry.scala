package com.lab.dualpool.metrics

import com.lab.dualpool.DatabaseRole
import com.zaxxer.hikari.metrics.MetricsTrackerFactory
import org.slf4j.LoggerFactory

/** Per-pool metric registration and reading (proposal §5.5). */
trait PoolMetricsRegistry {

  /**
   * Attached to every HikariCP pool the library builds, and to an adopted pool when the service hands over its
   * DataSource. This is what produces the `hikaricp_*` families; None means no Hikari metrics are exported.
   */
  def metricsTrackerFactory: Option[MetricsTrackerFactory]

  /**
   * Registers all [[PoolMetrics.MetricNames]] for the pool under a distinct, stable name
   * ("Per-pool naming"). Must run eagerly at pool creation, not lazily on first query.
   *
   * @param strict whether [[verifyRegistered]] may fail the service over this pool. True for a pool the library
   *               built, because it controls that pool completely. False for an adopted pool, whose lifecycle
   *               belongs to the service: a pool that has not been started yet cannot publish, and refusing to
   *               boot over it would make adoption harder than leaving the library out.
   */
  def register(role: DatabaseRole, stats: PoolStatsSource, strict: Boolean): Unit

  /**
   * "Registration timing": fails if any strict pool is missing any metric, so an unused pool is not healthy by
   * omission. A non-strict pool's gaps are logged instead.
   */
  def verifyRegistered(): Unit

  def snapshot(role: DatabaseRole): PoolMetrics

  def snapshotAll(): Map[DatabaseRole, PoolMetrics]

  /** Drops this pool's series. Called from `DualPool.close()` so a restarted pool does not register twice. */
  def unregister(role: DatabaseRole): Unit
}

object PoolMetricsRegistry {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * The Prometheus registry when `io.prometheus:simpleclient` is on the classpath, otherwise a no-op.
   *
   * simpleclient is a `Provided` dependency: the library must not force a metrics stack on a service that does
   * not have one, and a service that does gets its pools reported into the registry it already scrapes.
   */
  def default: PoolMetricsRegistry =
    try {
      Class.forName("io.prometheus.client.CollectorRegistry", false, getClass.getClassLoader)
      PrometheusPoolMetricsRegistry.forRegistry()
    } catch {
      case _: ClassNotFoundException | _: NoClassDefFoundError =>
        logger.warn("dualpool: io.prometheus.client not on the classpath; pool metrics are not exported. " +
          "Add io.prometheus:simpleclient to get per-pool observability (proposal section 5.5).")
        NoOpPoolMetricsRegistry
    }
}

/** Used when no metrics stack is available. Snapshots still work, so a service can log or expose them itself. */
object NoOpPoolMetricsRegistry extends PoolMetricsRegistry {

  private val sources = new java.util.concurrent.ConcurrentHashMap[DatabaseRole, PoolStatsSource]()

  def metricsTrackerFactory: Option[MetricsTrackerFactory] = None

  def register(role: DatabaseRole, stats: PoolStatsSource, strict: Boolean): Unit = { sources.put(role, stats); () }

  def verifyRegistered(): Unit = ()

  def snapshot(role: DatabaseRole): PoolMetrics = {
    val stats = sources.get(role)
    if (stats == null) throw new IllegalArgumentException(s"dualpool: no pool registered for role $role")
    PoolMetrics.from(stats, PoolMetrics.Unavailable.toLong)
  }

  def snapshotAll(): Map[DatabaseRole, PoolMetrics] = {
    import scala.jdk.CollectionConverters._
    sources.asScala.map { case (role, stats) => role -> PoolMetrics.from(stats, PoolMetrics.Unavailable.toLong) }.toMap
  }

  def unregister(role: DatabaseRole): Unit = { sources.remove(role); () }
}
