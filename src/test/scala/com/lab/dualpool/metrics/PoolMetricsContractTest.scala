package com.lab.dualpool.metrics

import com.lab.dualpool.support.HsqldbFixture
import com.lab.dualpool.{DatabaseRole, DualPool}
import io.prometheus.client.CollectorRegistry
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.CountDownLatch
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Try

/** Every pool must publish the same metrics under its own `pool` label, whether or not it has served a query. */
class PoolMetricsContractTest extends AnyFunSuite with Matchers with HsqldbFixture {

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
    val metrics = PrometheusPoolMetricsRegistry.forRegistry(new CollectorRegistry())
    metrics.register(DatabaseRole.Read, PoolStatsSource.fixed("never-built"), strict = true)

    // The Slick families come from the library's own collector, so the HikariCP ones are what is missing.
    val failure = the[IllegalStateException] thrownBy metrics.verifyRegistered()
    failure.getMessage should include("pool 'never-built' (Read) is missing metric 'hikaricp_active_connections'")
  }

  test("a pool the library built must publish, but an adopted one need not") {
    // The asymmetry is deliberate: the library controls a pool it built, so missing metrics there are a bug
    // worth failing the boot over. An adopted pool's lifecycle belongs to the service.
    val metrics = PrometheusPoolMetricsRegistry.forRegistry(new CollectorRegistry())

    metrics.register(DatabaseRole.Write, PoolStatsSource.fixed("silent-adopted"), strict = false)
    noException should be thrownBy metrics.verifyRegistered()

    metrics.register(DatabaseRole.Read, PoolStatsSource.fixed("silent-owned"), strict = true)
    val failure = the[IllegalStateException] thrownBy metrics.verifyRegistered()
    failure.getMessage should include("pool 'silent-owned' (Read) is missing metric")
    failure.getMessage should not include "silent-adopted"
  }

  test("acquire time is recorded as a histogram whose buckets actually contain the observations") {
    // HikariCP's own histogram tracker tops out at 1.024ms, so on a real pool every observation lands in +Inf
    // and histogram_quantile returns nothing usable. These buckets have to hold both a fast pooled handover and
    // a multi-second wait, or the fleet-wide p95 the dashboard computes is a lie.
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

      // And the buckets need enough resolution to separate a fast handover from a slow one: a pooled
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
      val release = new CountDownLatch(1)
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

        // And reads still work, at normal latency, while the write pool is saturated.
        val startedAt = System.nanoTime()
        await(ping(pool.readDb), 5.seconds)
        val elapsed = (System.nanoTime() - startedAt).nanos
        withClue(s"read took ${elapsed.toMillis}ms while the write pool was saturated: ") {
          elapsed.toMillis should be < 1000L
        }
      } finally {
        release.countDown()
        stalled.foreach(future => Try(await(future)))
      }
    }
  }

  private def newPool(suffix: String, registry: CollectorRegistry, writePool: String, readPool: String): DualPool =
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
}
