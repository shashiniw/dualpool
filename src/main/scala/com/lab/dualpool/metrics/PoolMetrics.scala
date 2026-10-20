package com.lab.dualpool.metrics

/** Point-in-time view of one pool. Every pool exposes exactly this set (proposal §5.5). */
final case class PoolMetrics(poolName: String,
                             activeConnections: Int,
                             idleConnections: Int,
                             pendingConnections: Int,
                             totalConnections: Int,
                             executorQueueDepth: Int,
                             connectionWaitMillisP95: Long)

object PoolMetrics {

  /**
   * The metric names every pool must expose, each carrying a `pool` label. The first six come from HikariCP's
   * own Prometheus exporter; `slick_executor_queue_size` is the library's, and is the one to alert on (§5.5).
   *
   * The contract test asserts that every pool publishes every one of these under its own `pool` label, including
   * a pool that has not yet served a query.
   */
  val MetricNames: Seq[String] = Seq(
    "hikaricp_active_connections",
    "hikaricp_idle_connections",
    "hikaricp_pending_threads",
    "hikaricp_connections",
    "hikaricp_connection_acquired_nanos",
    "hikaricp_connection_timeout_total",
    "slick_executor_queue_size"
  )

  /** Value used when a reading is not available, rather than a zero that would read as healthy. */
  val Unavailable: Int = -1

  def from(stats: PoolStatsSource, connectionWaitMillisP95: Long): PoolMetrics =
    PoolMetrics(
      poolName = stats.poolName,
      activeConnections = stats.activeConnections,
      idleConnections = stats.idleConnections,
      pendingConnections = stats.pendingConnections,
      totalConnections = stats.totalConnections,
      executorQueueDepth = stats.executorQueueDepth,
      connectionWaitMillisP95 = connectionWaitMillisP95
    )
}
