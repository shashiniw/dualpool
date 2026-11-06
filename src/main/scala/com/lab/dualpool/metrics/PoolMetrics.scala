package com.lab.dualpool.metrics

/** Point-in-time view of one pool. Every pool exposes exactly this set. */
final case class PoolMetrics(poolName: String,
                             activeConnections: Int,
                             idleConnections: Int,
                             pendingConnections: Int,
                             totalConnections: Int,
                             executorQueueDepth: Int,
                             connectionWaitMillisP95: Long)

object PoolMetrics {

  /** Value used when a reading is not available, rather than a zero that would read as healthy. */
  val Unavailable: Int = -1

  /**
   * The metric names every pool must publish, each carrying a `pool` label. The `hikaricp_*` names come from
   * HikariCP's own Prometheus exporter; the rest are this library's.
   *
   * `verifyRegistered` and the contract test assert that every pool publishes every one of these, including a
   * pool that has not yet served a query — a metric missing entirely reads as healthy on a dashboard.
   */
  val MetricNames: Seq[String] = Seq(
    "hikaricp_active_connections",
    "hikaricp_idle_connections",
    "hikaricp_pending_threads",
    "hikaricp_connections",
    "hikaricp_connection_acquired_nanos",
    "hikaricp_connection_timeout_total",
    "slick_executor_queue_size",
    ConnectionAcquireHistogram.MetricName)

  def from(stats: PoolStatsSource, connectionWaitMillisP95: Long): PoolMetrics =
    PoolMetrics(
      poolName = stats.poolName,
      activeConnections = stats.activeConnections,
      idleConnections = stats.idleConnections,
      pendingConnections = stats.pendingConnections,
      totalConnections = stats.totalConnections,
      executorQueueDepth = stats.executorQueueDepth,
      connectionWaitMillisP95 = connectionWaitMillisP95)
}
