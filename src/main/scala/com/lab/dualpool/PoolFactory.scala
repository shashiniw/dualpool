package com.lab.dualpool

import com.lab.dualpool.config.PoolConfig
import com.zaxxer.hikari.metrics.MetricsTrackerFactory
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import slick.jdbc.JdbcBackend
import slick.util.AsyncExecutor

/** Builds the pieces of a single pool from its [[PoolConfig]] (proposal §4.2 "Pool construction"). */
private[dualpool] object PoolFactory {

  /** HikariCP pool: poolName = `poolConfig.name`, readOnly, maxConnections and connectionTimeout applied. */
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
    poolConfig.idleTimeout.foreach(idle => hikariConfig.setIdleTimeout(idle.toMillis))
    hikariConfig.setReadOnly(poolConfig.readOnly)
    hikariConfig.setConnectionTestQuery(poolConfig.validationQuery)
    metricsTrackerFactory.foreach(hikariConfig.setMetricsTrackerFactory)
    // Constructing from a HikariConfig starts the pool now rather than on the first getConnection, so a bad
    // url or credential fails at startup and the metrics below have something to report from the start.
    new HikariDataSource(hikariConfig)
  }

  /**
   * Slick executor with numThreads and a *bounded* queue of queueSize, so overload is rejected fast (§5.4).
   *
   * `registerMbeans = true` is what makes the queue readable: Slick publishes
   * `slick:type=AsyncExecutor,name=<poolConfig.name>` exposing the queue size and active threads, and that is
   * the source behind `slick_executor_queue_size`. Slick registers it lazily on first use, so the execution
   * context is forced here — an idle pool must still report (§5.5 "Registration timing").
   */
  def executor(poolConfig: PoolConfig, decorator: AsyncExecutor => AsyncExecutor): AsyncExecutor = {
    val executor = AsyncExecutor(
      name = poolConfig.name,
      minThreads = poolConfig.numThreads,
      maxThreads = poolConfig.numThreads,
      queueSize = poolConfig.queueSize,
      maxConnections = poolConfig.maxConnections,
      registerMbeans = true)
    executor.executionContext
    decorator(executor)
  }

  /**
   * `maxConnections` is passed to Slick as well as to Hikari: Slick uses it to stop dispatching more
   * connection-holding work than the pool can serve, which is what keeps the queue meaningful.
   */
  def database(dataSource: HikariDataSource,
               executor: AsyncExecutor,
               poolConfig: PoolConfig): JdbcBackend#Database =
    JdbcBackend.Database.forDataSource(dataSource, Some(poolConfig.maxConnections), executor)
}
