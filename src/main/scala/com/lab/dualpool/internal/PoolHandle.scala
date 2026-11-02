package com.lab.dualpool.internal

import com.lab.dualpool.DatabaseRole
import com.lab.dualpool.config.PoolConfig
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import slick.jdbc.JdbcBackend
import slick.util.AsyncExecutor

import java.util.concurrent.atomic.LongAdder

/**
 * One pool and everything the library needs in order to close it or report on it.
 *
 * @param owned        false for an adopted pool, which the library reports on but never closes
 * @param executorName the Slick executor's name, which differs from `config.name` only for an adopted pool
 * @param rejections   the counter behind `slick_executor_rejected_total`, when there is one
 */
private[dualpool] final case class PoolHandle(role: DatabaseRole,
                                              config: PoolConfig,
                                              dataSource: Option[HikariDataSource],
                                              executor: Option[AsyncExecutor],
                                              database: JdbcBackend#Database,
                                              owned: Boolean,
                                              executorName: String,
                                              rejections: Option[LongAdder] = None) {

  /**
   * Closing the Slick database shuts the executor down but leaves the DataSource alone, because Slick did not
   * open it. Both have to go, in that order.
   */
  def shutdown(): Unit = {
    try database.close()
    catch { case e: Throwable => PoolHandle.logger.warn("dualpool: closing database '{}': {}", config.name, e.getMessage) }

    dataSource.foreach { dataSource =>
      try dataSource.close()
      catch { case e: Throwable => PoolHandle.logger.warn("dualpool: closing pool '{}': {}", config.name, e.getMessage) }
    }
  }
}

private[dualpool] object PoolHandle {
  private val logger = LoggerFactory.getLogger(classOf[PoolHandle])
}
