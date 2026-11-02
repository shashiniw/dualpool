package com.lab.dualpool.internal

import com.lab.dualpool.config.PoolConfig
import com.zaxxer.hikari.metrics.MetricsTrackerFactory
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import slick.jdbc.JdbcBackend
import slick.util.AsyncExecutor

import java.util.concurrent.atomic.LongAdder

/** Builds the three pieces of one pool: the HikariCP DataSource, the Slick executor and the Slick database. */
private[dualpool] object PoolFactory {

  /**
   * The pool's HikariCP DataSource.
   *
   * Constructing from a `HikariConfig` starts the pool immediately rather than on the first `getConnection`,
   * so a bad url or credential fails at startup and the pool's metrics have something to report from the start.
   */
  def hikari(poolConfig: PoolConfig, metricsTrackerFactory: Option[MetricsTrackerFactory]): HikariDataSource = {
    val hikariConfig = new HikariConfig()
    hikariConfig.setPoolName(poolConfig.name)
    hikariConfig.setJdbcUrl(poolConfig.url)
    if (poolConfig.driver.nonEmpty) hikariConfig.setDriverClassName(poolConfig.driver)
    poolConfig.username.foreach(hikariConfig.setUsername)
    poolConfig.password.foreach(hikariConfig.setPassword)
    hikariConfig.setMaximumPoolSize(poolConfig.maxConnections)
    poolConfig.minIdle.foreach(hikariConfig.setMinimumIdle)
    hikariConfig.setConnectionTimeout(poolConfig.connectionTimeout.toMillis)
    poolConfig.idleTimeout.foreach(idleTimeout => hikariConfig.setIdleTimeout(idleTimeout.toMillis))
    hikariConfig.setReadOnly(poolConfig.readOnly)
    hikariConfig.setConnectionTestQuery(poolConfig.validationQuery)
    metricsTrackerFactory.foreach(hikariConfig.setMetricsTrackerFactory)
    new HikariDataSource(hikariConfig)
  }

  /**
   * The pool's Slick executor, with a bounded queue of `queueSize` so overload is rejected rather than
   * accumulated.
   *
   * `registerMbeans = true` is what makes the queue readable: Slick then publishes
   * `slick:type=AsyncExecutor,name=<poolName>`, the source behind `slick_executor_queue_size`. Slick registers
   * it lazily on first use, so the execution context is forced here — an idle pool must still report.
   */
  def executor(poolConfig: PoolConfig,
               decorator: AsyncExecutor => AsyncExecutor,
               rejections: LongAdder): AsyncExecutor = {
    val executor = AsyncExecutor(
      name = poolConfig.name,
      minThreads = poolConfig.numThreads,
      maxThreads = poolConfig.numThreads,
      queueSize = poolConfig.queueSize,
      maxConnections = poolConfig.maxConnections,
      registerMbeans = true)
    executor.executionContext
    decorator(RejectionCountingExecutor(executor, rejections))
  }

  /**
   * The Slick database for this pool.
   *
   * `maxConnections` is passed to Slick as well as to HikariCP: Slick uses it to stop dispatching more
   * connection-holding work than the pool can serve, which is what keeps the queue metric meaningful.
   */
  def database(dataSource: HikariDataSource,
               executor: AsyncExecutor,
               poolConfig: PoolConfig): JdbcBackend#Database =
    JdbcBackend.Database.forDataSource(dataSource, Some(poolConfig.maxConnections), executor)
}
