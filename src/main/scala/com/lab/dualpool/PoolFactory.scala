package com.lab.dualpool

import com.lab.dualpool.config.PoolConfig
import com.zaxxer.hikari.HikariDataSource
import slick.jdbc.{JdbcBackend, JdbcProfile}
import slick.util.AsyncExecutor

/** Builds the pieces of a single pool from its [[PoolConfig]] (proposal §4.2 "Pool construction"). */
private[dualpool] object PoolFactory {

  /** HikariCP pool: poolName = `poolConfig.name`, readOnly, maxConnections and connectionTimeout applied. */
  def hikari(poolConfig: PoolConfig): HikariDataSource = ???

  /** Slick executor with numThreads and a *bounded* queue of queueSize, so overload is rejected fast (§5.4). */
  def executor(poolConfig: PoolConfig): AsyncExecutor = ???

  def database(profile: JdbcProfile,
               dataSource: HikariDataSource,
               executor: AsyncExecutor,
               poolConfig: PoolConfig): JdbcBackend#Database = ???
}
