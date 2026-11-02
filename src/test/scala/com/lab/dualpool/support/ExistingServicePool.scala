package com.lab.dualpool.support

import com.lab.dualpool.DualPool
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import slick.jdbc.JdbcBackend
import slick.util.AsyncExecutor

import java.util.concurrent.atomic.LongAdder

/**
 * Stands in for what a service already owns before it adopts the library: its own HikariCP pool, its own Slick
 * executor and the Slick database built from the two. The adoption tests hand this to
 * `DualPool.adoptingWritePool`.
 */
final class ExistingServicePool private (val dataSource: HikariDataSource,
                                         val executorName: String,
                                         val rejections: LongAdder,
                                         val database: JdbcBackend#Database) extends AutoCloseable {

  def close(): Unit = {
    database.close()
    dataSource.close()
  }
}

object ExistingServicePool {

  /**
   * @param startEagerly   true builds the pool from a `HikariConfig`, which starts it immediately. False uses
   *                       setters, which leaves it unstarted until the first `getConnection` — the case the
   *                       library has to warm up before the pool will publish any metrics.
   * @param registerMbeans false leaves the executor's size unreadable, as a service that never enabled JMX.
   * @param countRejections wraps the executor with `DualPool.countingRejections`, as a service would in order
   *                       to report `slick_executor_rejected_total` for its own pool.
   */
  def apply(url: String,
            poolName: Option[String],
            executorName: String,
            maxConnections: Int = 4,
            executorThreads: Int = 4,
            queueSize: Int = 1000,
            startEagerly: Boolean = true,
            registerMbeans: Boolean = true,
            countRejections: Boolean = false): ExistingServicePool = {
    val dataSource = if (startEagerly) eager(url, poolName, maxConnections) else lazily(url, poolName, maxConnections)

    val rejections = new LongAdder
    val plain = AsyncExecutor(executorName, executorThreads, executorThreads, queueSize, executorThreads, registerMbeans)
    val executor = if (countRejections) DualPool.countingRejections(plain, rejections) else plain

    val database = JdbcBackend.Database.forDataSource(dataSource, Some(maxConnections), executor)
    new ExistingServicePool(dataSource, executorName, rejections, database)
  }

  private def eager(url: String, poolName: Option[String], maxConnections: Int): HikariDataSource = {
    val config = new HikariConfig()
    poolName.foreach(config.setPoolName)
    config.setJdbcUrl(url)
    config.setDriverClassName(Hsqldb.Driver)
    config.setUsername(Hsqldb.Username)
    config.setMaximumPoolSize(maxConnections)
    config.setConnectionTestQuery(Hsqldb.TestQuery)
    new HikariDataSource(config)
  }

  private def lazily(url: String, poolName: Option[String], maxConnections: Int): HikariDataSource = {
    val dataSource = new HikariDataSource()
    poolName.foreach(dataSource.setPoolName)
    dataSource.setJdbcUrl(url)
    dataSource.setDriverClassName(Hsqldb.Driver)
    dataSource.setUsername(Hsqldb.Username)
    dataSource.setMaximumPoolSize(maxConnections)
    dataSource.setConnectionTestQuery(Hsqldb.TestQuery)
    dataSource
  }
}
