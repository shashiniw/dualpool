package com.lab.dualpool

import com.lab.dualpool.config.{DualPoolConfig, PoolConfigValidator}
import com.lab.dualpool.internal.{AdoptedPool, PoolBuilder, PoolHandle, RejectionCountingExecutor}
import com.lab.dualpool.metrics.{PoolMetrics, PoolMetricsRegistry}
import com.zaxxer.hikari.HikariDataSource
import slick.jdbc.{JdbcBackend, JdbcProfile}
import slick.util.AsyncExecutor

import java.util.concurrent.atomic.LongAdder

/**
 * A service's database pools, one per workload. Each pool has its own HikariCP connection pool, its own Slick
 * executor and bounded queue, and its own metrics, so a stalled write cannot starve reads of connections.
 *
 * A DAO opts in by using [[readDb]] instead of [[writeDb]]; anything not migrated keeps using [[writeDb]].
 */
trait DualPool extends AutoCloseable {

  /** The write pool, from the `db` section. Existing code keeps using this unchanged. */
  def writeDb: JdbcBackend#Database

  /** The read pool, from the `readDb` section. The same instance as [[writeDb]] when `readDb` is not configured. */
  def readDb: JdbcBackend#Database

  /** The optional background-job pool, from the `jobDb` section. Falls back to [[writeDb]] when not configured. */
  def backgroundDb: JdbcBackend#Database

  /** True when [[readDb]] is a pool of its own rather than the write pool. */
  def isReadPoolIsolated: Boolean

  def metrics: PoolMetricsRegistry

  def metricsSnapshot: Map[DatabaseRole, PoolMetrics]

  /** Closes every pool this instance built. Safe to call more than once; never closes an adopted pool. */
  override def close(): Unit
}

object DualPool {

  /**
   * Builds the pools described by `config`: validates sizing, creates one DataSource and one executor per pool,
   * registers per-pool metrics, and runs each pool's validation query so a broken pool fails here rather than
   * on a user's first request.
   *
   * Nothing is created until this is called, and the caller closes the result with the service.
   *
   * @param executorDecorator wraps each pool's executor, for MDC propagation and the like
   */
  def apply(profile: JdbcProfile,
            config: DualPoolConfig,
            executorDecorator: AsyncExecutor => AsyncExecutor = identity,
            metricsRegistry: PoolMetricsRegistry = PoolMetricsRegistry.default): DualPool = {
    PoolConfigValidator.validateOrFail(config, config.instances, config.databaseConnectionLimit)

    val handles = PoolBuilder.build(config.pools, Map.empty, executorDecorator, metricsRegistry)
    PoolBuilder.start(profile, handles, metricsRegistry)
  }

  /** [[DualPoolConfig.load]] followed by [[apply]]. */
  def fromConfig(profile: JdbcProfile,
                 config: com.typesafe.config.Config,
                 executorDecorator: AsyncExecutor => AsyncExecutor = identity): DualPool =
    apply(profile, DualPoolConfig.load(config), executorDecorator)

  /**
   * Incremental adoption for a service that already owns a write pool: reuses `existingWriteDb` as
   * [[DualPool.writeDb]], left untouched and not closed by [[DualPool.close]], and builds only the read and
   * background pools from `config`. Nothing about the existing `db` wiring changes.
   *
   * @param adoptedWriteDataSource   the service's own HikariCP DataSource. Optional, and it changes nothing
   *                                 about how that pool behaves; it only lets the write pool be reported
   *                                 alongside the read pool, which is what makes the two comparable.
   * @param adoptedWriteExecutorName the name of the service's own Slick executor, when it was built with
   *                                 `registerMbeans = true`. Without it the write pool's queue depth reports
   *                                 as unavailable (-1) rather than as a misleading zero.
   * @param adoptedWriteRejections   the counter the service's executor was wrapped with via
   *                                 [[countingRejections]]. Without it the write pool does not report
   *                                 `slick_executor_rejected_total`.
   */
  def adoptingWritePool(profile: JdbcProfile,
                        existingWriteDb: JdbcBackend#Database,
                        config: DualPoolConfig,
                        executorDecorator: AsyncExecutor => AsyncExecutor = identity,
                        adoptedWriteDataSource: Option[HikariDataSource] = None,
                        adoptedWriteExecutorName: Option[String] = None,
                        adoptedWriteRejections: Option[LongAdder] = None,
                        metricsRegistry: PoolMetricsRegistry = PoolMetricsRegistry.default): DualPool = {
    val adoptedConfig = config.copy(write = AdoptedPool.effectiveConfig(
      config.write, existingWriteDb, adoptedWriteDataSource, adoptedWriteExecutorName))

    PoolConfigValidator.validateOrFail(adoptedConfig, adoptedConfig.instances, adoptedConfig.databaseConnectionLimit)

    val adopted = PoolHandle(
      role = DatabaseRole.Write,
      config = adoptedConfig.write,
      dataSource = adoptedWriteDataSource,
      executor = None,
      database = existingWriteDb,
      owned = false,
      executorName = adoptedWriteExecutorName.getOrElse(adoptedConfig.write.name),
      rejections = adoptedWriteRejections)

    AdoptedPool.attachMetrics(adopted, metricsRegistry)

    val toBuild = adoptedConfig.pools.filterNot { case (role, _) => role == DatabaseRole.Write }
    val handles = PoolBuilder.build(toBuild, Map(DatabaseRole.Write -> adopted), executorDecorator, metricsRegistry)
    PoolBuilder.start(profile, handles, metricsRegistry)
  }

  /**
   * Wraps a service's own executor so the work its bounded queue refuses is counted in `rejections`. Pass the
   * same counter to [[adoptingWritePool]] as `adoptedWriteRejections` to report it for the adopted write pool.
   */
  def countingRejections(executor: AsyncExecutor, rejections: LongAdder): AsyncExecutor =
    RejectionCountingExecutor(executor, rejections)
}
