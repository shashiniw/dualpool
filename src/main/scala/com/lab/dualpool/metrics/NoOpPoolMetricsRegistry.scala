package com.lab.dualpool.metrics

import com.lab.dualpool.DatabaseRole
import com.zaxxer.hikari.metrics.MetricsTrackerFactory

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

/**
 * Used when no metrics stack is on the classpath. Nothing is exported, but snapshots still work, so a service
 * can log or expose the numbers itself.
 */
object NoOpPoolMetricsRegistry extends PoolMetricsRegistry {

  private val sources = new ConcurrentHashMap[DatabaseRole, PoolStatsSource]()

  def metricsTrackerFactory: Option[MetricsTrackerFactory] = None

  def register(role: DatabaseRole, stats: PoolStatsSource, strict: Boolean): Unit = {
    sources.put(role, stats)
    ()
  }

  def verifyRegistered(): Unit = ()

  def snapshot(role: DatabaseRole): PoolMetrics =
    Option(sources.get(role))
      .map(PoolMetrics.from(_, PoolMetrics.Unavailable.toLong))
      .getOrElse(throw new IllegalArgumentException(s"dualpool: no pool registered for role $role"))

  def snapshotAll(): Map[DatabaseRole, PoolMetrics] =
    sources.asScala.map { case (role, stats) =>
      role -> PoolMetrics.from(stats, PoolMetrics.Unavailable.toLong)
    }.toMap

  def unregister(role: DatabaseRole): Unit = {
    sources.remove(role)
    ()
  }
}
