package com.lab.dualpool.metrics

import com.zaxxer.hikari.HikariDataSource
import slick.util.AsyncExecutorMXBean

import java.lang.management.ManagementFactory
import java.util.concurrent.atomic.LongAdder
import javax.management.{JMX, ObjectName}

/**
 * Live readings for one pool. Separating this from the registry is what lets the metric code be tested without
 * a database and lets a service plug in its own reporting (proposal §5.5).
 *
 * Every accessor is defensive: a pool that has not finished starting reports -1 rather than throwing, because a
 * metrics read must never be able to take down the thing it is measuring.
 */
trait PoolStatsSource {
  def poolName: String
  def activeConnections: Int
  def idleConnections: Int
  def pendingConnections: Int
  def totalConnections: Int

  /** Requests waiting for a thread in the Slick executor. The clearest saturation signal (§5.5). */
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

  private val Unavailable = -1

  /**
   * Reads HikariCP through its own MXBean object and the Slick executor through the MBean Slick registers as
   * `slick:type=AsyncExecutor,name=<poolName>` when built with `registerMbeans = true`.
   *
   * Slick registers that MBean when the executor's thread pool is first touched, not when the executor is
   * constructed, which is exactly the "registration timing" pitfall in §5.5. `PoolFactory` forces it at
   * construction and `PoolMetricsRegistry.verifyRegistered()` proves it, so an idle pool is never reported as
   * healthy by omission.
   *
   * @param executorName the Slick executor name, which the library keeps equal to the Hikari pool name
   */
  def apply(dataSource: HikariDataSource, executorName: String, rejections: Option[LongAdder] = None): PoolStatsSource =
    new HikariSlickStatsSource(dataSource, executorName, rejections)

  private final class HikariSlickStatsSource(dataSource: HikariDataSource,
                                             executorName: String,
                                             rejections: Option[LongAdder]) extends PoolStatsSource {

    private val objectName = new ObjectName(s"slick:type=AsyncExecutor,name=$executorName")

    // Resolved on demand and cached: the MBean may not exist yet if a caller reads metrics before the pool
    // finished starting, and we do not want to keep paying for a failed lookup once it does exist.
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
    def executorRejected: Long = rejections.map(_.sum).getOrElse(Unavailable.toLong)

    private def hikari(read: com.zaxxer.hikari.HikariPoolMXBean => Int): Int =
      try {
        Option(dataSource.getHikariPoolMXBean).map(read).getOrElse(Unavailable)
      } catch {
        case _: Throwable => Unavailable
      }

    private def executor(read: AsyncExecutorMXBean => Int): Int =
      try {
        resolveExecutorBean().map(read).getOrElse(Unavailable)
      } catch {
        case _: Throwable => Unavailable
      }

    private def resolveExecutorBean(): Option[AsyncExecutorMXBean] = executorBean match {
      case some @ Some(_) => some
      case None =>
        val server = ManagementFactory.getPlatformMBeanServer
        if (server.isRegistered(objectName)) {
          val proxy = JMX.newMXBeanProxy(server, objectName, classOf[AsyncExecutorMXBean])
          executorBean = Some(proxy)
          executorBean
        } else None
    }
  }

  /** For tests and for services that report their own numbers. */
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
}
