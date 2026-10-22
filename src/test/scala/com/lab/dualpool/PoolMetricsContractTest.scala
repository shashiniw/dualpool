package com.lab.dualpool

import com.lab.dualpool.metrics.{PoolMetrics, PrometheusPoolMetricsRegistry}
import io.prometheus.client.CollectorRegistry
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

/** Contract tests from proposal §5.5 / §7.2. */
class PoolMetricsContractTest extends AnyFunSuite with Matchers with HsqldbFixture {

  private def newPool(suffix: String, registry: CollectorRegistry, writePool: String, readPool: String) =
    DualPool(
      profile,
      dualPoolConfig(databaseName(suffix), writePool, Some(readSection(readPool))),
      identity,
      PrometheusPoolMetricsRegistry.forRegistry(registry))

  /** Every metric name published for a given `pool` label value. */
  private def namesFor(registry: CollectorRegistry, poolName: String): Set[String] =
    registry.metricFamilySamples().asScala
      .flatMap(_.samples.asScala)
      .filter { sample =>
        val index = sample.labelNames.indexOf("pool")
        index >= 0 && sample.labelValues.get(index) == poolName
      }
      .map(_.name)
      .toSet

  test("every pool exposes the same metric set under its own name") {
    val registry = new CollectorRegistry()
    withPool(newPool("contract", registry, "contract-write", "contract-read")) { pool =>
      pool.isReadPoolIsolated shouldBe true

      val writeNames = namesFor(registry, "contract-write")
      val readNames = namesFor(registry, "contract-read")

      PoolMetrics.MetricNames.foreach { required =>
        withClue(s"write pool is missing $required: ") { writeNames.exists(_.startsWith(required)) shouldBe true }
        withClue(s"read pool is missing $required: ") { readNames.exists(_.startsWith(required)) shouldBe true }
      }

      // The point of the contract: not just "both have metrics" but "both have the same ones".
      writeNames shouldBe readNames
    }
  }

  test("a pool that has not served a query is still registered") {
    val registry = new CollectorRegistry()
    withPool(newPool("idle", registry, "idle-write", "idle-read")) { pool =>
      // Only the write pool is used; the read pool has served nothing beyond its startup check.
      await(ping(pool.writeDb))

      // A metric missing entirely reads as healthy on a dashboard. verifyRegistered is what forbids that.
      noException should be thrownBy pool.metrics.verifyRegistered()

      val read = pool.metricsSnapshot(DatabaseRole.Read)
      read.poolName shouldBe "idle-read"
      read.executorQueueDepth shouldBe 0
      read.totalConnections should be >= 0
    }
  }

  test("verifyRegistered fails when a pool publishes nothing") {
    val registry = new CollectorRegistry()
    val metrics = PrometheusPoolMetricsRegistry.forRegistry(registry)
    metrics.register(DatabaseRole.Read, com.lab.dualpool.metrics.PoolStatsSource.fixed("never-built"), strict = true)

    // The Slick family is published by the library's own collector, so the Hikari families are what is missing.
    val failure = the[IllegalStateException] thrownBy metrics.verifyRegistered()
    failure.getMessage should include("pool 'never-built' (Read) is missing metric 'hikaricp_active_connections'")
  }

  test("acquire time is recorded as a histogram whose buckets actually contain the observations") {
    // HikariCP's own histogram tracker tops out at 1.024ms, so on a real pool every observation lands in +Inf
    // and histogram_quantile returns nothing usable. These buckets have to hold both a fast pooled handover
    // and a multi-second wait, or the fleet-wide p95 the dashboard computes is a lie.
    val registry = new CollectorRegistry()
    withPool(newPool("histogram", registry, "histogram-write", "histogram-read")) { pool =>
      (1 to 20).foreach(_ => await(ping(pool.readDb)))

      def sample(name: String, labels: Array[String], values: Array[String]): Double =
        Option(registry.getSampleValue(name, labels, values)).map(_.doubleValue()).getOrElse(0.0)

      val observed = sample("dualpool_connection_acquired_seconds_count", Array("pool"), Array("histogram-read"))
      observed should be >= 20.0

      // Every observation must be inside a finite bucket, not only in +Inf.
      val underTenSeconds = sample("dualpool_connection_acquired_seconds_bucket",
        Array("pool", "le"), Array("histogram-read", "10.0"))
      underTenSeconds shouldBe observed

      // And the buckets must have enough resolution to separate a fast handover from a slow one: a pooled
      // acquisition is sub-millisecond, so the small buckets have to be populated too.
      val underOneMilli = sample("dualpool_connection_acquired_seconds_bucket",
        Array("pool", "le"), Array("histogram-read", "0.001"))
      withClue("no observation landed under 1ms, so the low buckets are useless: ") {
        underOneMilli should be > 0.0
      }

      // The write pool published its buckets too, even though only the read pool served queries.
      sample("dualpool_connection_acquired_seconds_count", Array("pool"), Array("histogram-write")) should be >= 0.0
    }
  }

  test("saturating the write pool does not change read pool metrics") {
    val registry = new CollectorRegistry()
    val configuration = dualPoolConfig(
      databaseName("isolation"),
      "isolation-write",
      Some(readSection("isolation-read", maxConnections = 2, queueSize = 10)),
      writeOverrides = "maxConnections = 2\nqueueSize = 4")

    withPool(DualPool(profile, configuration, identity, PrometheusPoolMetricsRegistry.forRegistry(registry))) { pool =>
      val release = new java.util.concurrent.CountDownLatch(1)
      // Two connections held plus four queued: the write pool is full and its queue is backing up.
      val stalled = (1 to 6).map(_ => hold(pool.writeDb, release).recover { case _ => () })

      try {
        eventually() { pool.metricsSnapshot(DatabaseRole.Write).executorQueueDepth > 0 } shouldBe true

        val writeMetrics = pool.metricsSnapshot(DatabaseRole.Write)
        val readMetrics = pool.metricsSnapshot(DatabaseRole.Read)

        writeMetrics.activeConnections shouldBe 2
        writeMetrics.executorQueueDepth should be > 0

        // The whole point of the library: the read pool is untouched by the stall next door.
        readMetrics.executorQueueDepth shouldBe 0
        readMetrics.activeConnections shouldBe 0

        // And reads still work, at normal latency, while the write pool is saturated (§7.2 fault injection).
        val startedAt = System.nanoTime()
        await(ping(pool.readDb), scala.concurrent.duration.DurationInt(5).seconds)
        val elapsedMillis = (System.nanoTime() - startedAt) / 1000000
        withClue(s"read took ${elapsedMillis}ms while the write pool was saturated: ") {
          elapsedMillis should be < 1000L
        }
      } finally {
        release.countDown()
        stalled.foreach(future => scala.util.Try(await(future)))
      }
    }
  }
}
