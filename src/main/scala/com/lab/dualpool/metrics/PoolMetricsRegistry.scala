package com.lab.dualpool.metrics

import com.lab.dualpool.DatabaseRole

/** Per-pool metric registration and reading (proposal §5.5). */
trait PoolMetricsRegistry {

  /**
   * Registers all [[PoolMetrics.MetricNames]] for the pool under a distinct, stable name
   * ("Per-pool naming"). Must run eagerly at pool creation, not lazily on first query.
   */
  def register(role: DatabaseRole, poolName: String): Unit

  /** "Registration timing": fails if any pool is missing any metric, so an unused pool is not healthy by omission. */
  def verifyRegistered(): Unit

  def snapshot(role: DatabaseRole): PoolMetrics

  def snapshotAll(): Map[DatabaseRole, PoolMetrics]
}
