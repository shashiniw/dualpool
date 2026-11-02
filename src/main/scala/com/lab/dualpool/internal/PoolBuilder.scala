package com.lab.dualpool.internal

import com.lab.dualpool.config.PoolConfig
import com.lab.dualpool.metrics.{PoolMetricsRegistry, PoolStatsSource}
import com.lab.dualpool.{DatabaseRole, DualPool}
import org.slf4j.LoggerFactory
import slick.jdbc.JdbcProfile
import slick.util.AsyncExecutor

import java.util.concurrent.atomic.LongAdder
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

/** Creates the pools, registers their metrics and proves each one works before handing back a [[DualPool]]. */
private[dualpool] object PoolBuilder {

  private val logger = LoggerFactory.getLogger(getClass)

  /** How much longer than its own connection timeout a pool gets to answer the startup query. */
  private val ProbeGracePeriod = 5.seconds

  /**
   * Builds one pool per entry in `pools`, on top of any `alreadyBuilt` handles (an adopted write pool).
   *
   * A failure part way through gives back the pools created so far before rethrowing: half-built is worse
   * than not built.
   */
  def build(pools: Seq[(DatabaseRole, PoolConfig)],
            alreadyBuilt: Map[DatabaseRole, PoolHandle],
            executorDecorator: AsyncExecutor => AsyncExecutor,
            metricsRegistry: PoolMetricsRegistry): Map[DatabaseRole, PoolHandle] =
    pools.foldLeft(alreadyBuilt) { case (built, (role, poolConfig)) =>
      try built + (role -> buildOne(role, poolConfig, executorDecorator, metricsRegistry))
      catch {
        case e: Throwable =>
          built.values.filter(_.owned).foreach(_.shutdown())
          throw new IllegalStateException(
            s"dualpool: could not build the ${role.defaultPoolName} pool '${poolConfig.name}': ${e.getMessage}", e)
      }
    }

  /**
   * Registers every pool's metrics, proves every pool the library built can serve a query, and returns the
   * assembled [[DualPool]]. Anything that fails here is cleaned up before the exception propagates.
   */
  def start(profile: JdbcProfile,
            handles: Map[DatabaseRole, PoolHandle],
            metricsRegistry: PoolMetricsRegistry): DualPool = {
    handles.values.foreach(registerMetrics(_, metricsRegistry))

    try {
      handles.values.filter(_.owned).foreach(probe(profile, _))
      metricsRegistry.verifyRegistered()
    } catch {
      case e: Throwable =>
        handles.values.filter(_.owned).foreach(_.shutdown())
        handles.keys.foreach(metricsRegistry.unregister)
        throw e
    }

    val pool = new DualPoolImpl(handles, metricsRegistry)
    logger.info("dualpool: started {} pool(s): {}. Read pool isolated: {}",
      Int.box(handles.size),
      handles.values.map(handle => s"${handle.role.defaultPoolName}='${handle.config.name}'").mkString(", "),
      Boolean.box(pool.isReadPoolIsolated))
    pool
  }

  private def buildOne(role: DatabaseRole,
                       poolConfig: PoolConfig,
                       executorDecorator: AsyncExecutor => AsyncExecutor,
                       metricsRegistry: PoolMetricsRegistry): PoolHandle = {
    val dataSource = PoolFactory.hikari(poolConfig, metricsRegistry.metricsTrackerFactory)
    val rejections = new LongAdder
    val executor = PoolFactory.executor(poolConfig, executorDecorator, rejections)

    PoolHandle(
      role = role,
      config = poolConfig,
      dataSource = Some(dataSource),
      executor = Some(executor),
      database = PoolFactory.database(dataSource, executor, poolConfig),
      owned = true,
      executorName = poolConfig.name,
      rejections = Some(rejections))
  }

  /** Only a pool the library built is allowed to fail the start over missing metrics. */
  private def registerMetrics(handle: PoolHandle, metricsRegistry: PoolMetricsRegistry): Unit =
    handle.dataSource.foreach { dataSource =>
      metricsRegistry.register(
        handle.role,
        PoolStatsSource(dataSource, handle.executorName, handle.rejections),
        strict = handle.owned)
    }

  /**
   * Runs the pool's validation query through the full Slick path at startup.
   *
   * This proves three things at once that would otherwise surface as a failed user request: the credentials
   * work, the executor's thread pool is up, and the executor MBean behind `slick_executor_queue_size` is
   * registered.
   */
  private def probe(profile: JdbcProfile, handle: PoolHandle): Unit = {
    import profile.api._

    val action = SimpleDBIO[Unit] { context =>
      val statement = context.connection.createStatement()
      try statement.execute(handle.config.validationQuery) finally statement.close()
    }

    try Await.result(handle.database.run(action), handle.config.connectionTimeout + ProbeGracePeriod)
    catch {
      case e: Throwable =>
        throw new IllegalStateException(s"dualpool: startup check '${handle.config.validationQuery}' failed on " +
          s"pool '${handle.config.name}': ${e.getMessage}", e)
    }
  }
}
