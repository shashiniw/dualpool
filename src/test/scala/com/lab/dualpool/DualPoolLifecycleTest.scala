package com.lab.dualpool

import com.lab.dualpool.config.{DualPoolConfig, PoolConfigValidator}
import com.lab.dualpool.internal.AdoptedPool
import com.lab.dualpool.metrics.PrometheusPoolMetricsRegistry
import com.lab.dualpool.support.{ExistingServicePool, Hsqldb, HsqldbFixture}
import com.typesafe.config.ConfigFactory
import io.prometheus.client.CollectorRegistry
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.CountDownLatch
import scala.concurrent.duration._
import scala.util.Try

class DualPoolLifecycleTest extends AnyFunSuite with Matchers with HsqldbFixture {

  test("without a readDb section readDb is the write pool itself, not a copy of it") {
    withPool(DualPool(profile, dualPoolConfig(databaseName("fallback"), "fallback-write"))) { pool =>
      pool.readDb should be theSameInstanceAs pool.writeDb
      pool.backgroundDb should be theSameInstanceAs pool.writeDb
      pool.isReadPoolIsolated shouldBe false
      await(ping(pool.readDb))
    }
  }

  test("close is idempotent and leaves the pools unusable afterwards") {
    val pool = DualPool(profile, dualPoolConfig(databaseName("close"), "close-write", Some(readSection("close-read"))))
    await(ping(pool.readDb))

    pool.close()
    noException should be thrownBy pool.close()

    a[Exception] should be thrownBy await(ping(pool.readDb), 5.seconds)
  }

  test("a bad url fails at construction, not on the first query") {
    val configuration = DualPoolConfig.load(ConfigFactory.parseString(
      s"""db {
         |  url = "${Hsqldb.url("probe")}"
         |  driver = "${Hsqldb.Driver}"
         |  username = "${Hsqldb.Username}"
         |  poolName = "probe-write"
         |  validationQuery = "${Hsqldb.TestQuery}"
         |}
         |readDb {
         |  url = "jdbc:hsqldb:hsql://127.0.0.1:1/nope"
         |  poolName = "probe-read"
         |  maxConnections = 2
         |  connectionTimeout = 1s
         |}""".stripMargin))

    val failure = the[Exception] thrownBy DualPool(profile, configuration)
    failure.getMessage should include("dualpool: could not build the read pool 'probe-read'")
  }

  test("adoptingWritePool reuses the service's pool, reports it, and never closes it") {
    val name = databaseName("adopt")
    val registry = new CollectorRegistry()
    val existing = ExistingServicePool(
      Hsqldb.url(name), Some("adopted-write"), "adopted-executor", countRejections = true)

    try {
      val configuration = DualPoolConfig.load(ConfigFactory.parseString(
        s"""db { url = "${Hsqldb.url(name)}", driver = "${Hsqldb.Driver}", username = "${Hsqldb.Username}", maxPoolSize = 4 }
           |readDb {
           |  poolName = "adopted-read"
           |  maxConnections = 2
           |  queueSize = 10
           |  validationQuery = "${Hsqldb.TestQuery}"
           |}""".stripMargin))

      val pool = adopt(existing, configuration, registry, rejections = true)
      try {
        pool.writeDb should be theSameInstanceAs existing.database
        pool.isReadPoolIsolated shouldBe true

        // Both pools report, which is what makes read and write comparable on one dashboard.
        noException should be thrownBy pool.metrics.verifyRegistered()
        pool.metricsSnapshot(DatabaseRole.Write).poolName shouldBe "adopted-write"
        await(ping(pool.writeDb))
        pool.metricsSnapshot(DatabaseRole.Write).executorQueueDepth should be >= 0

        // The service's executor was wrapped with the library's counter, so the write pool reports rejections too.
        existing.rejections.add(2)
        registry.getSampleValue("slick_executor_rejected_total", Array("pool"), Array("adopted-write")) shouldBe 2.0
      } finally pool.close()

      // The adopted pool survives close(): the service built it and the service closes it.
      existing.dataSource.isClosed shouldBe false
      await(ping(existing.database))
    } finally existing.close()
  }

  test("an adopted write pool with the read pool disabled gets metrics but no second pool") {
    // The state a service deploys first: readDb { enabled = false }. Reads still go to the write pool, so
    // behaviour is unchanged, but the write pool is now reported and there is a before-picture to compare to.
    val name = databaseName("metrics-only")
    val registry = new CollectorRegistry()
    val existing = ExistingServicePool(Hsqldb.url(name), Some("metrics-only-write"), "metrics-only-executor")

    try {
      val configuration = DualPoolConfig.load(ConfigFactory.parseString(
        s"""db { url = "${Hsqldb.url(name)}", driver = "${Hsqldb.Driver}", username = "${Hsqldb.Username}", maxPoolSize = 4 }
           |readDb { enabled = false, maxConnections = 2 }""".stripMargin))
      configuration.read shouldBe None

      val pool = adopt(existing, configuration, registry)
      try {
        pool.isReadPoolIsolated shouldBe false
        pool.readDb should be theSameInstanceAs existing.database

        await(ping(pool.writeDb))
        noException should be thrownBy pool.metrics.verifyRegistered()

        val writeMetrics = pool.metricsSnapshot(DatabaseRole.Write)
        writeMetrics.poolName shouldBe "metrics-only-write"
        // Queue depth is readable because the service's own executor registers its MBean.
        writeMetrics.executorQueueDepth shouldBe 0
        writeMetrics.totalConnections should be > 0
      } finally pool.close()

      existing.dataSource.isClosed shouldBe false
    } finally existing.close()
  }

  test("an adopted pool that has not started yet still reports, because the library starts it") {
    // A pool built with setters rather than from a HikariConfig does not start until its first getConnection,
    // and HikariCP registers a pool with its exporter only when it starts. Without the warm-up this combination
    // failed the service start with "pool metrics are not fully registered".
    val name = databaseName("lazy-adopt")
    val registry = new CollectorRegistry()
    val existing = ExistingServicePool(
      Hsqldb.url(name), Some("lazy-adopted-write"), "lazy-adopted-executor", startEagerly = false)

    try {
      existing.dataSource.isRunning shouldBe false

      val configuration = DualPoolConfig.load(ConfigFactory.parseString(
        s"""db {
           |  url = "${Hsqldb.url(name)}"
           |  driver = "${Hsqldb.Driver}"
           |  username = "${Hsqldb.Username}"
           |  maxPoolSize = 4
           |  preferredTestQuery = "${Hsqldb.TestQuery}"
           |}
           |readDb { poolName = "lazy-adopted-read", maxConnections = 2, queueSize = 10 }""".stripMargin))

      val pool = adopt(existing, configuration, registry)
      try {
        pool.metricsSnapshot(DatabaseRole.Write).totalConnections should be > 0
        pool.metricsSnapshot(DatabaseRole.Read).poolName shouldBe "lazy-adopted-read"
      } finally pool.close()
    } finally existing.close()
  }

  test("an adopted pool is checked as the service built it, not as its config describes it") {
    // `db.maxPoolSize = 6` implies 6 threads, but the executor is sized in code with 20 threads and a 1000
    // queue. The mismatch is a warning, so the service still starts.
    val name = databaseName("actual-sizing")
    val existing = ExistingServicePool(
      Hsqldb.url(name), Some("actual-sizing-write"), "actual-sizing-executor",
      maxConnections = 6, executorThreads = 20, startEagerly = false)

    try {
      val configuration = DualPoolConfig.load(ConfigFactory.parseString(
        s"""db { url = "${Hsqldb.url(name)}", maxPoolSize = 6 }"""))

      val write = AdoptedPool.effectiveConfig(
        configuration.write, existing.database, Some(existing.dataSource), Some(existing.executorName))

      (write.maxConnections, write.numThreads, write.queueSize) shouldBe ((6, 20, 1000))
      PoolConfigValidator.validate(configuration.copy(write = write)) shouldBe Seq(PoolConfigValidator.Warning(
        "db (pool 'actual-sizing-write'): numThreads (20) != maxConnections (6); surplus threads block inside " +
          "Slick waiting for a connection, which hides saturation from the queue metric"))
    } finally existing.close()
  }

  test("an adopted executor without MBeans cannot be checked, so it is not reported as mismatched") {
    val name = databaseName("no-mbeans")
    val existing = ExistingServicePool(
      Hsqldb.url(name), poolName = None, "no-mbeans-executor",
      executorThreads = 20, startEagerly = false, registerMbeans = false)

    try {
      val configuration = DualPoolConfig.load(ConfigFactory.parseString(
        s"""db { url = "${Hsqldb.url(name)}", maxPoolSize = 4 }"""))

      val write = AdoptedPool.effectiveConfig(
        configuration.write, existing.database, Some(existing.dataSource), Some(existing.executorName))

      (write.maxConnections, write.numThreads, write.queueSize) shouldBe ((4, 4, configuration.write.queueSize))
      PoolConfigValidator.validate(configuration.copy(write = write)) shouldBe empty
    } finally existing.close()
  }

  test("the read queue is bounded, so overload is refused rather than accumulated") {
    val configuration = dualPoolConfig(
      databaseName("overload"),
      "overload-write",
      Some(readSection("overload-read", maxConnections = 1, queueSize = 1)))

    withPool(DualPool(profile, configuration)) { pool =>
      val release = new CountDownLatch(1)
      val blocker = hold(pool.readDb, release).recover { case _ => () }

      try {
        eventually() { pool.metricsSnapshot(DatabaseRole.Read).activeConnections == 1 } shouldBe true

        val flood = (1 to 50).map(_ => ping(pool.readDb))
        def failures = flood.count(_.value.exists(_.isFailure))

        withClue("a bounded queue must reject once it is full rather than growing without limit: ") {
          eventually(10.seconds) { failures > 0 } shouldBe true
        }

        val rejected = failures
        CollectorRegistry.defaultRegistry.getSampleValue(
          "slick_executor_rejected_total", Array("pool"), Array("overload-read")) shouldBe rejected.toDouble
      } finally {
        release.countDown()
        Try(await(blocker))
      }
    }
  }

  private def adopt(existing: ExistingServicePool,
                    configuration: DualPoolConfig,
                    registry: CollectorRegistry,
                    rejections: Boolean = false): DualPool =
    DualPool.adoptingWritePool(
      profile, existing.database, configuration,
      adoptedWriteDataSource = Some(existing.dataSource),
      adoptedWriteExecutorName = Some(existing.executorName),
      adoptedWriteRejections = if (rejections) Some(existing.rejections) else None,
      metricsRegistry = PrometheusPoolMetricsRegistry.forRegistry(registry))
}
