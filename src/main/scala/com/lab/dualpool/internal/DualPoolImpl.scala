package com.lab.dualpool.internal

import com.lab.dualpool.metrics.{PoolMetrics, PoolMetricsRegistry}
import com.lab.dualpool.{DatabaseRole, DualPool}
import org.slf4j.LoggerFactory
import slick.jdbc.JdbcBackend

import java.util.concurrent.atomic.AtomicBoolean

private[dualpool] final class DualPoolImpl(handles: Map[DatabaseRole, PoolHandle],
                                           val metrics: PoolMetricsRegistry) extends DualPool {

  private val closed = new AtomicBoolean(false)

  val writeDb: JdbcBackend#Database = handles(DatabaseRole.Write).database

  /** The fallback the backward-compatibility story rests on: no `readDb` section, same object as `db`. */
  val readDb: JdbcBackend#Database = handles.get(DatabaseRole.Read).map(_.database).getOrElse(writeDb)

  val backgroundDb: JdbcBackend#Database = handles.get(DatabaseRole.Background).map(_.database).getOrElse(writeDb)

  def isReadPoolIsolated: Boolean = !(readDb eq writeDb)

  def metricsSnapshot: Map[DatabaseRole, PoolMetrics] = metrics.snapshotAll()

  def close(): Unit =
    if (closed.compareAndSet(false, true)) {
      handles.values.filter(_.owned).foreach { handle =>
        DualPoolImpl.logger.info("dualpool: closing {} pool '{}'", handle.role.defaultPoolName, handle.config.name)
        handle.shutdown()
      }
      handles.keys.foreach(metrics.unregister)
    }
}

private[dualpool] object DualPoolImpl {
  private val logger = LoggerFactory.getLogger(classOf[DualPoolImpl])
}
