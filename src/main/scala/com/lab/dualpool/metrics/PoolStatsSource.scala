package com.lab.dualpool.metrics

import com.zaxxer.hikari.{HikariDataSource, HikariPoolMXBean}
import slick.util.AsyncExecutorMXBean

import java.lang.management.ManagementFactory
import java.util.concurrent.atomic.LongAdder
import javax.management.{JMX, ObjectName}

/**
 * Live readings for one pool. Keeping this separate from the registry is what lets the metric code be tested
 * without a database, and lets a service plug in its own reporting.
 *
 * Every accessor is defensive: a pool that has not finished starting reports [[PoolMetrics.Unavailable]] rather
 * than throwing, because reading a metric must never be able to take down the thing it measures.
 */
trait PoolStatsSource {

  def poolName: String
  def activeConnections: Int
  def idleConnections: Int
  def pendingConnections: Int
  def totalConnections: Int

  /** Requests waiting for a thread in the Slick executor. The clearest saturation signal. */
  def executorQueueDepth: Int

  /** The bound on that queue, so a dashboard can show depth as a fraction of capacity. */
  def executorQueueCapacity: Int

  def executorActiveThreads: Int

  /** The executor's thread count, so an adopted pool's threads can be checked against its connections. */
  def executorMaxThreads: Int

  /** Work refused because the executor queue was full, or -1 for an executor the library did not build. */
  def executorRejected: Long
}

object PoolStatsSource {

  /**
   * Reads HikariCP through its own MXBean and the Slick executor through the MBean Slick registers as
   * `slick:type=AsyncExecutor,name=<executorName>` when built with `registerMbeans = true`.
   *
   * @param executorName the Slick executor name, which the library keeps equal to the HikariCP pool name
   * @param rejections   the counter behind `executorRejected`, for an executor the library wrapped
   */
  def apply(dataSource: HikariDataSource,
            executorName: String,
            rejections: Option[LongAdder] = None): PoolStatsSource =
    new JmxPoolStatsSource(dataSource, executorName, rejections)

  /** Fixed readings, for tests and for services that report their own numbers. */
  def fixed(name: String,
            active: Int = 0,
            idle: Int = 0,
            pending: Int = 0,
            total: Int = 0,
            queueDepth: Int = 0,
            queueCapacity: Int = 0,
            activeThreads: Int = 0,
            maxThreads: Int = 0,
            rejected: Long = 0): PoolStatsSource = new PoolStatsSource {
    val poolName: String = name
    val activeConnections: Int = active
    val idleConnections: Int = idle
    val pendingConnections: Int = pending
    val totalConnections: Int = total
    val executorQueueDepth: Int = queueDepth
    val executorQueueCapacity: Int = queueCapacity
    val executorActiveThreads: Int = activeThreads
    val executorMaxThreads: Int = maxThreads
    val executorRejected: Long = rejected
  }

  private final class JmxPoolStatsSource(dataSource: HikariDataSource,
                                         executorName: String,
                                         rejections: Option[LongAdder]) extends PoolStatsSource {

    private val objectName = new ObjectName(s"slick:type=AsyncExecutor,name=$executorName")

    // Resolved on demand and then cached: the MBean may not exist yet if something reads metrics before the
    // pool has finished starting, and a failed lookup should not be paid for again once it does exist.
    @volatile private var executorBean: Option[AsyncExecutorMXBean] = None

    def poolName: String = dataSource.getPoolName

    def activeConnections: Int = hikari(_.getActiveConnections)
    def idleConnections: Int = hikari(_.getIdleConnections)
    def pendingConnections: Int = hikari(_.getThreadsAwaitingConnection)
    def totalConnections: Int = hikari(_.getTotalConnections)

    def executorQueueDepth: Int = executor(_.getQueueSize)
    def executorQueueCapacity: Int = executor(_.getMaxQueueSize)
    def executorActiveThreads: Int = executor(_.getActiveThreads)
    def executorMaxThreads: Int = executor(_.getMaxThreads)
    def executorRejected: Long = rejections.map(_.sum).getOrElse(PoolMetrics.Unavailable.toLong)

    private def hikari(read: HikariPoolMXBean => Int): Int =
      try Option(dataSource.getHikariPoolMXBean).map(read).getOrElse(PoolMetrics.Unavailable)
      catch { case _: Throwable => PoolMetrics.Unavailable }

    private def executor(read: AsyncExecutorMXBean => Int): Int =
      try resolveExecutorBean().map(read).getOrElse(PoolMetrics.Unavailable)
      catch { case _: Throwable => PoolMetrics.Unavailable }

    private def resolveExecutorBean(): Option[AsyncExecutorMXBean] = executorBean match {
      case resolved @ Some(_) => resolved
      case None =>
        val server = ManagementFactory.getPlatformMBeanServer
        if (!server.isRegistered(objectName)) None
        else {
          executorBean = Some(JMX.newMXBeanProxy(server, objectName, classOf[AsyncExecutorMXBean]))
          executorBean
        }
    }
  }
}
