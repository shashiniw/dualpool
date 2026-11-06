package com.lab.dualpool.metrics

import com.zaxxer.hikari.metrics.{IMetricsTracker, MetricsTrackerFactory, PoolStats}
import io.prometheus.client.Histogram

/**
 * Wraps HikariCP's own Prometheus tracker rather than replacing it.
 *
 * The delegate is what registers a pool with HikariCP's collector, so dropping it would take
 * `hikaricp_active_connections` and friends with it. Every callback is forwarded; the only addition is one
 * extra observation per acquisition, into [[ConnectionAcquireHistogram]].
 */
private[metrics] final class HistogramMetricsTrackerFactory(delegate: MetricsTrackerFactory,
                                                            histogram: Histogram) extends MetricsTrackerFactory {

  def create(poolName: String, poolStats: PoolStats): IMetricsTracker =
    new HistogramMetricsTracker(delegate.create(poolName, poolStats), histogram.labels(poolName))
}

private[metrics] final class HistogramMetricsTracker(delegate: IMetricsTracker,
                                                     acquired: Histogram.Child) extends IMetricsTracker {

  private val NanosPerSecond = 1000000000.0

  override def recordConnectionAcquiredNanos(elapsedAcquiredNanos: Long): Unit = {
    acquired.observe(elapsedAcquiredNanos / NanosPerSecond)
    delegate.recordConnectionAcquiredNanos(elapsedAcquiredNanos)
  }

  override def recordConnectionCreatedMillis(connectionCreatedMillis: Long): Unit =
    delegate.recordConnectionCreatedMillis(connectionCreatedMillis)

  override def recordConnectionUsageMillis(elapsedBorrowedMillis: Long): Unit =
    delegate.recordConnectionUsageMillis(elapsedBorrowedMillis)

  override def recordConnectionTimeout(): Unit = delegate.recordConnectionTimeout()

  override def close(): Unit = delegate.close()
}
