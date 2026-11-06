package com.lab.dualpool.metrics

import com.lab.dualpool.DatabaseRole
import com.zaxxer.hikari.metrics.MetricsTrackerFactory
import org.slf4j.LoggerFactory

/** Registers and reads the metrics of each pool. */
trait PoolMetricsRegistry {

  /**
   * Attached to every HikariCP pool the library builds, and to an adopted pool when the service hands over its
   * DataSource. This is what produces the `hikaricp_*` families; None means no HikariCP metrics are exported.
   */
  def metricsTrackerFactory: Option[MetricsTrackerFactory]

  /**
   * Registers all of [[PoolMetrics.MetricNames]] for this pool under its own `pool` label. Runs eagerly at pool
   * creation, not lazily on first query.
   *
   * @param strict whether [[verifyRegistered]] may fail the service over this pool. True for a pool the library
   *               built, because it controls that pool completely. False for an adopted pool, whose lifecycle
   *               belongs to the service: a pool that has not been started yet cannot publish, and refusing to
   *               boot over it would make adoption harder than leaving the library out.
   */
  def register(role: DatabaseRole, stats: PoolStatsSource, strict: Boolean): Unit

  /**
   * Fails if any strict pool is missing any metric, so an unused pool is not reported as healthy by omission.
   * A non-strict pool's gaps are logged instead.
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
        logger.warn("dualpool: io.prometheus.client is not on the classpath, so pool metrics are not exported. " +
          "Add io.prometheus:simpleclient to get per-pool observability.")
        NoOpPoolMetricsRegistry
    }
}
