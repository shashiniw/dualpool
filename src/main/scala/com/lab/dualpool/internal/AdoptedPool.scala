package com.lab.dualpool.internal

import com.lab.dualpool.config.PoolConfig
import com.lab.dualpool.metrics.{PoolMetricsRegistry, PoolStatsSource}
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import slick.jdbc.JdbcBackend

import scala.util.Try

/** The two things the library has to do with a pool the service built itself: describe it, and make it report. */
private[dualpool] object AdoptedPool {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Replaces the configured sizes with the real ones read from the running pool, so validation checks what was
   * actually built rather than what the config guessed at. Threads fall back to connections when the executor
   * has no MBean to read them from.
   */
  def effectiveConfig(configured: PoolConfig,
                      existingWriteDb: JdbcBackend#Database,
                      dataSource: Option[HikariDataSource],
                      executorName: Option[String]): PoolConfig = {
    Try(existingWriteDb.executor.executionContext) // Slick registers the executor MBean on first use.

    val stats = dataSource.map(PoolStatsSource(_, executorName.getOrElse(configured.name)))
    val maxConnections = dataSource.map(_.getMaximumPoolSize).getOrElse(configured.maxConnections)

    configured.copy(
      name = dataSource.map(_.getPoolName).filter(_ != null).getOrElse(configured.name),
      maxConnections = maxConnections,
      numThreads = stats.map(_.executorMaxThreads).filter(_ > 0).getOrElse(maxConnections),
      queueSize = stats.map(_.executorQueueCapacity).filter(_ > 0).getOrElse(configured.queueSize),
      adopted = true)
  }

  /**
   * Attaches the metrics exporter to the already-running pool, which HikariCP supports, so the write pool can
   * be observed from the moment the read pool exists without restarting the service.
   */
  def attachMetrics(handle: PoolHandle, metricsRegistry: PoolMetricsRegistry): Unit =
    for {
      dataSource <- handle.dataSource
      factory <- metricsRegistry.metricsTrackerFactory
    } {
      try {
        dataSource.setMetricsTrackerFactory(factory)
        warmUp(dataSource, handle.config.name)
      } catch {
        case e: Throwable =>
          logger.warn("dualpool: could not attach metrics to the adopted write pool '{}': {}",
            handle.config.name, e.getMessage)
      }
    }

  /**
   * Starts an adopted pool so it publishes.
   *
   * A HikariDataSource built with setters rather than from a HikariConfig does not start until its first
   * `getConnection`, and HikariCP registers a pool with its Prometheus exporter only once the pool starts.
   * Without this the adopted write pool would report nothing until the service happened to issue its first query.
   *
   * Failure is logged, never fatal: the service could already boot with the database briefly unreachable, and
   * adopting the library must not change that.
   */
  private def warmUp(dataSource: HikariDataSource, poolName: String): Unit =
    try {
      val connection = dataSource.getConnection()
      connection.close()
    } catch {
      case e: Throwable =>
        logger.warn("dualpool: adopted pool '{}' could not be started, so its metrics will only appear once " +
          "the service issues its first query: {}", poolName, e.getMessage)
    }
}
