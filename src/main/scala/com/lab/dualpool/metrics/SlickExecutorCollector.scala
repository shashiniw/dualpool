package com.lab.dualpool.metrics

import io.prometheus.client.{Collector, CounterMetricFamily, GaugeMetricFamily}

import scala.jdk.CollectionConverters._

/**
 * Publishes the Slick executor's state, which Slick exposes over JMX but not to Prometheus.
 *
 * `slick_executor_queue_size` is the saturation signal to alert on: `hikaricp_pending_threads` can read zero
 * while this queue is deep, because work waits for an executor thread before it ever asks for a connection.
 */
private[metrics] final class SlickExecutorCollector(sources: () => Seq[PoolStatsSource]) extends Collector {

  private val PoolLabel = List("pool").asJava

  def collect(): java.util.List[Collector.MetricFamilySamples] = {
    val queueSize = new GaugeMetricFamily("slick_executor_queue_size",
      "Requests waiting in the Slick executor queue", PoolLabel)
    val queueCapacity = new GaugeMetricFamily("slick_executor_queue_capacity",
      "Bound on the Slick executor queue", PoolLabel)
    val activeThreads = new GaugeMetricFamily("slick_executor_active_threads",
      "Threads currently executing in the Slick executor", PoolLabel)
    val rejected = new CounterMetricFamily("slick_executor_rejected",
      "Work refused because the Slick executor queue was full", PoolLabel)

    sources().foreach { stats =>
      val labels = List(stats.poolName).asJava
      queueSize.addMetric(labels, stats.executorQueueDepth.toDouble)
      queueCapacity.addMetric(labels, stats.executorQueueCapacity.toDouble)
      activeThreads.addMetric(labels, stats.executorActiveThreads.toDouble)
      // Only an executor the library wrapped can be counted; no series at all beats a counter stuck at -1.
      if (stats.executorRejected >= 0) rejected.addMetric(labels, stats.executorRejected.toDouble)
    }

    List[Collector.MetricFamilySamples](queueSize, queueCapacity, activeThreads, rejected).asJava
  }
}
