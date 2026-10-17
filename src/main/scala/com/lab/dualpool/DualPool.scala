package com.lab.dualpool

import com.lab.dualpool.config.DualPoolConfig
import com.lab.dualpool.metrics.{PoolMetrics, PoolMetricsRegistry}
import slick.jdbc.{JdbcBackend, JdbcProfile}
import slick.util.AsyncExecutor

/**
 * Entry point of the library: independent Slick databases per workload, each with its own HikariCP pool,
 * Slick executor/queue and metrics (proposal §4.1, §4.2).
 *
 * A DAO opts in by using [[readDb]] instead of [[writeDb]]; anything not migrated keeps using [[writeDb]].
 */
trait DualPool extends AutoCloseable {

  /** The write pool (`db` section). Existing code keeps using this unchanged. */
  def writeDb: JdbcBackend#Database

  /** The read pool (`readDb` section). Same instance as [[writeDb]] when `readDb` is not configured. */
  def readDb: JdbcBackend#Database

  /** The optional background-job pool (`jobDb`). Falls back to [[writeDb]] when not configured. */
  def backgroundDb: JdbcBackend#Database

  def isReadPoolIsolated: Boolean

  def metrics: PoolMetricsRegistry

  def metricsSnapshot: Map[DatabaseRole, PoolMetrics]

  /** Closes every pool created by this instance; safe to call more than once. */
  override def close(): Unit
}

object DualPool {

  /**
   * Builds the pools described by `config`: validates sizing (fails fast), creates one HikariDataSource and one
   * AsyncExecutor per pool, and registers per-pool metrics eagerly. Lifecycle is explicit: nothing is created
   * until this is called, and the caller closes the result with the service (proposal §4.2 "Lifecycle").
   *
   * @param executorDecorator wraps each pool's executor, e.g. to propagate MDC as clearance's MdcAsyncExecutor does
   */
  def apply(profile: JdbcProfile,
            config: DualPoolConfig,
            executorDecorator: AsyncExecutor => AsyncExecutor = identity): DualPool = ???

  /**
   * Incremental adoption for services that already own a write pool: reuses `existingWriteDb` as [[DualPool.writeDb]]
   * (left untouched and NOT closed by [[DualPool.close]]) and builds only the read/background pools from
   * `config`. Nothing about the existing `db` wiring changes (proposal §5.7).
   */
  def adoptingWritePool(profile: JdbcProfile,
                        existingWriteDb: JdbcBackend#Database,
                        config: DualPoolConfig,
                        executorDecorator: AsyncExecutor => AsyncExecutor = identity): DualPool = ???

  /** Convenience: `DualPoolConfig.load(config)` followed by [[apply]]. */
  def fromConfig(profile: JdbcProfile,
                 config: com.typesafe.config.Config,
                 executorDecorator: AsyncExecutor => AsyncExecutor = identity): DualPool = ???
}
