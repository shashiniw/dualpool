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
  /** The stable metric names each pool registers under `<poolName>.<metric>`; used by the contract test. */
  val MetricNames: Seq[String] =
    Seq("active", "idle", "pending", "total", "queueDepth", "connectionWaitMillis")
}
