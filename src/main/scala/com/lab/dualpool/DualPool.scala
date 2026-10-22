package com.lab.dualpool

import com.lab.dualpool.config.{DualPoolConfig, PoolConfig, PoolConfigValidator}
import com.lab.dualpool.metrics.{PoolMetrics, PoolMetricsRegistry, PoolStatsSource}
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import slick.jdbc.{JdbcBackend, JdbcProfile}
import slick.util.AsyncExecutor

import java.util.concurrent.atomic.{AtomicBoolean, LongAdder}
import scala.util.Try

/**
 * Entry point of the library: independent Slick databases per workload, each with its own HikariCP pool,
 * Slick executor/queue and metrics (proposal §4.1, §4.2).
 *
 * A DAO opts in by using [[readDb]] instead of [[writeDb]]; anything not migrated keeps using [[writeDb]].
 */
trait DualPool extends AutoCloseable {

  /** The write pool (`db` section). Existing code keeps using this unchanged. */
  def writeDb: JdbcBackend#Database

  /** The read pool (`readDb` section). Same instance as [[writeDb]] when `readDb` is not configured. */
  def readDb: JdbcBackend#Database

  /** The optional background-job pool (`jobDb`). Falls back to [[writeDb]] when not configured. */
  def backgroundDb: JdbcBackend#Database

  def isReadPoolIsolated: Boolean

  def metrics: PoolMetricsRegistry

  def metricsSnapshot: Map[DatabaseRole, PoolMetrics]

  /** Closes every pool created by this instance; safe to call more than once. */
  override def close(): Unit
}

object DualPool {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Builds the pools described by `config`: validates sizing (fails fast), creates one HikariDataSource and one
   * AsyncExecutor per pool, and registers per-pool metrics eagerly. Lifecycle is explicit: nothing is created
   * until this is called, and the caller closes the result with the service (proposal §4.2 "Lifecycle").
   *
   * @param executorDecorator wraps each pool's executor
   */
  def apply(profile: JdbcProfile,
            config: DualPoolConfig,
            executorDecorator: AsyncExecutor => AsyncExecutor = identity,
            metricsRegistry: PoolMetricsRegistry = PoolMetricsRegistry.default): DualPool = {
    PoolConfigValidator.validateOrFail(config, config.instances, config.databaseConnectionLimit)

    val handles = build(profile, config.pools, Map.empty, executorDecorator, metricsRegistry)
    finish(profile, handles, metricsRegistry)
  }

  /**
   * Incremental adoption for services that already own a write pool: reuses `existingWriteDb` as [[DualPool.writeDb]]
   * (left untouched and NOT closed by [[DualPool.close]]) and builds only the read/background pools from
   * `config`. Nothing about the existing `db` wiring changes (proposal §5.7).
   *
   * @param adoptedWriteDataSource  the service's own HikariCP DataSource. Passing it is optional and changes
   *                                nothing about how the pool behaves; it only lets the write pool be reported
   *                                alongside the read pool, which is what makes the two comparable on a dashboard.
   * @param adoptedWriteExecutorName the name of the service's own Slick executor, when it was built with
   *                                `registerMbeans = true`. Without it the write pool's queue depth reports as
   *                                unavailable (-1) rather than as a misleading zero.
   * @param adoptedWriteRejections  the counter the service's executor was wrapped with via [[countingRejections]].
   *                                Without it the write pool does not report `slick_executor_rejected_total`.
   */
  def adoptingWritePool(profile: JdbcProfile,
                        existingWriteDb: JdbcBackend#Database,
                        config: DualPoolConfig,
                        executorDecorator: AsyncExecutor => AsyncExecutor = identity,
                        adoptedWriteDataSource: Option[HikariDataSource] = None,
                        adoptedWriteExecutorName: Option[String] = None,
                        adoptedWriteRejections: Option[LongAdder] = None,
                        metricsRegistry: PoolMetricsRegistry = PoolMetricsRegistry.default): DualPool = {
    val adoptedConfig = config.copy(write =
      adoptedWriteConfig(config.write, existingWriteDb, adoptedWriteDataSource, adoptedWriteExecutorName))

    PoolConfigValidator.validateOrFail(adoptedConfig, adoptedConfig.instances, adoptedConfig.databaseConnectionLimit)

    val adoptedHandle = PoolHandle(
      role = DatabaseRole.Write,
      config = adoptedConfig.write,
      dataSource = adoptedWriteDataSource,
      executor = None,
      database = existingWriteDb,
      owned = false,
      executorName = adoptedWriteExecutorName.getOrElse(adoptedConfig.write.name),
      rejections = adoptedWriteRejections)

    // Attaching the exporter to a pool that is already running is supported by HikariCP, so the write pool can
    // be observed from the moment the read pool exists without restarting it.
    adoptedWriteDataSource.foreach { dataSource =>
      metricsRegistry.metricsTrackerFactory.foreach { factory =>
        try {
          dataSource.setMetricsTrackerFactory(factory)
          warmUp(dataSource, adoptedConfig.write.name)
        } catch {
          case e: Throwable =>
            logger.warn("dualpool: could not attach metrics to the adopted write pool '{}': {}",
              adoptedConfig.write.name, e.getMessage)
        }
      }
    }

    val toBuild = adoptedConfig.pools.filterNot(_._1 == DatabaseRole.Write)
    val handles = build(profile, toBuild, Map(DatabaseRole.Write -> adoptedHandle), executorDecorator, metricsRegistry)
    finish(profile, handles, metricsRegistry)
  }

  /**
   * Wraps a service's own executor so the work its bounded queue refuses is counted in `rejections`. Pass the
   * same counter to [[adoptingWritePool]] as `adoptedWriteRejections` to report it for the adopted write pool.
   */
  def countingRejections(executor: AsyncExecutor, rejections: LongAdder): AsyncExecutor =
    PoolFactory.countingRejections(executor, rejections)

  /** Convenience: `DualPoolConfig.load(config)` followed by [[apply]]. */
  def fromConfig(profile: JdbcProfile,
                 config: com.typesafe.config.Config,
                 executorDecorator: AsyncExecutor => AsyncExecutor = identity): DualPool =
    apply(profile, DualPoolConfig.load(config), executorDecorator)

  private def build(profile: JdbcProfile,
                    pools: Seq[(DatabaseRole, PoolConfig)],
                    initial: Map[DatabaseRole, PoolHandle],
                    executorDecorator: AsyncExecutor => AsyncExecutor,
                    metricsRegistry: PoolMetricsRegistry): Map[DatabaseRole, PoolHandle] =
    pools.foldLeft(initial) { case (built, (role, poolConfig)) =>
      try {
        val dataSource = PoolFactory.hikari(poolConfig, metricsRegistry.metricsTrackerFactory)
        val rejections = new LongAdder
        val executor = PoolFactory.executor(poolConfig, executorDecorator, rejections)
        val database = PoolFactory.database(dataSource, executor, poolConfig)
        built + (role -> PoolHandle(role, poolConfig, Some(dataSource), Some(executor), database,
          owned = true, executorName = poolConfig.name, rejections = Some(rejections)))
      } catch {
        case e: Throwable =>
          // Half-built is worse than not built: give back the pools created so far before rethrowing.
          built.values.filter(_.owned).foreach(_.shutdown())
          throw new IllegalStateException(
            s"dualpool: could not build the ${role.defaultPoolName} pool '${poolConfig.name}': ${e.getMessage}", e)
      }
    }

  /**
   * Replaces the adopted pool's configured sizes with the real ones read from the running pool, so validation
   * checks what was actually built. Threads fall back to connections when the executor has no MBean.
   */
  private[dualpool] def adoptedWriteConfig(configured: PoolConfig,
                                           existingWriteDb: JdbcBackend#Database,
                                           dataSource: Option[HikariDataSource],
                                           executorName: Option[String]): PoolConfig = {
    Try(existingWriteDb.executor.executionContext) // Slick registers the executor MBean on first use
    val stats = dataSource.map(PoolStatsSource(_, executorName.getOrElse(configured.name)))
    val maxConnections = dataSource.map(_.getMaximumPoolSize).getOrElse(configured.maxConnections)
    configured.copy(
      // Report the pool under the name it actually carries, not the one the config guessed at.
      name = dataSource.map(_.getPoolName).filter(_ != null).getOrElse(configured.name),
      maxConnections = maxConnections,
      numThreads = stats.map(_.executorMaxThreads).filter(_ > 0).getOrElse(maxConnections),
      queueSize = stats.map(_.executorQueueCapacity).filter(_ > 0).getOrElse(configured.queueSize),
      adopted = true)
  }

  /**
   * Starts an adopted HikariCP pool so it publishes.
   *
   * A HikariDataSource built with setters rather than from a HikariConfig does not start until its first
   * `getConnection`, and HikariCP registers a pool with its Prometheus exporter only when the pool starts. So
   * without this the adopted write pool reports nothing until the service happens to issue its first query.
   *
   * Failure is logged, never fatal: the service could boot with the database briefly unreachable before the
   * library existed, and adopting the library must not change that.
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

  private def finish(profile: JdbcProfile,
                     handles: Map[DatabaseRole, PoolHandle],
                     metricsRegistry: PoolMetricsRegistry): DualPool = {
    handles.values.foreach { handle =>
      handle.dataSource.foreach { dataSource =>
        // Only a pool the library built is allowed to fail the start over missing metrics.
        metricsRegistry.register(handle.role, PoolStatsSource(dataSource, handle.executorName, handle.rejections), strict = handle.owned)
      }
    }

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
      Int.box(handles.size), handles.values.map(h => s"${h.role.defaultPoolName}='${h.config.name}'").mkString(", "),
      Boolean.box(pool.isReadPoolIsolated))
    pool
  }

  /**
   * Runs the pool's validation query through the full Slick path at startup.
   *
   * This is the `start()` check from the plan. It proves three things at once that would otherwise surface as a
   * failed user request: the credentials work, the executor's thread pool is up, and the executor MBean behind
   * `slick_executor_queue_size` is registered.
   */
  private def probe(profile: JdbcProfile, handle: PoolHandle): Unit = {
    import profile.api._
    import scala.concurrent.Await
    import scala.concurrent.duration._

    val timeout = handle.config.connectionTimeout + 5.seconds
    val action = SimpleDBIO[Unit] { context =>
      val statement = context.connection.createStatement()
      try statement.execute(handle.config.validationQuery) finally statement.close()
    }
    try Await.result(handle.database.run(action), timeout)
    catch {
      case e: Throwable =>
        throw new IllegalStateException(
          s"dualpool: startup check '${handle.config.validationQuery}' failed on pool '${handle.config.name}': ${e.getMessage}", e)
    }
  }

  /** One pool and everything the library has to be able to close or report on. */
  private final case class PoolHandle(role: DatabaseRole,
                                      config: PoolConfig,
                                      dataSource: Option[HikariDataSource],
                                      executor: Option[AsyncExecutor],
                                      database: JdbcBackend#Database,
                                      owned: Boolean,
                                      executorName: String,
                                      rejections: Option[LongAdder] = None) {

    /**
     * Closing the Slick database shuts the executor down but deliberately leaves the DataSource alone, because
     * Slick did not open it. Both have to go, in that order.
     */
    def shutdown(): Unit = {
      try database.close() catch { case e: Throwable => logger.warn("dualpool: closing database '{}': {}", config.name, e.getMessage) }
      dataSource.foreach { dataSource =>
        try dataSource.close() catch { case e: Throwable => logger.warn("dualpool: closing pool '{}': {}", config.name, e.getMessage) }
      }
    }
  }

  private final class DualPoolImpl(handles: Map[DatabaseRole, PoolHandle],
                                   val metrics: PoolMetricsRegistry) extends DualPool {

    private val closed = new AtomicBoolean(false)

    val writeDb: JdbcBackend#Database = handles(DatabaseRole.Write).database

    /** The fallback the whole backward-compatibility story rests on: no `readDb` section, same object as `db`. */
    val readDb: JdbcBackend#Database = handles.get(DatabaseRole.Read).map(_.database).getOrElse(writeDb)

    val backgroundDb: JdbcBackend#Database = handles.get(DatabaseRole.Background).map(_.database).getOrElse(writeDb)

    def isReadPoolIsolated: Boolean = !(readDb eq writeDb)

    def metricsSnapshot: Map[DatabaseRole, PoolMetrics] = metrics.snapshotAll()

    def close(): Unit = {
      if (closed.compareAndSet(false, true)) {
        handles.values.filter(_.owned).foreach { handle =>
          logger.info("dualpool: closing {} pool '{}'", handle.role.defaultPoolName, handle.config.name)
          handle.shutdown()
        }
        handles.keys.foreach(metrics.unregister)
      }
    }
  }
}
