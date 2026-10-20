package com.lab.dualpool

import com.lab.dualpool.config.DualPoolConfig
import com.lab.dualpool.metrics.{PoolStatsSource, PrometheusPoolMetricsRegistry}
import com.typesafe.config.ConfigFactory
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import io.prometheus.client.CollectorRegistry
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import slick.jdbc.JdbcBackend
import slick.util.AsyncExecutor

import scala.concurrent.duration._

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
      """db {
        |  url = "jdbc:hsqldb:mem:ok"
        |  driver = "org.hsqldb.jdbc.JDBCDriver"
        |  username = "sa"
        |  poolName = "probe-write"
        |  validationQuery = "SELECT 1 FROM INFORMATION_SCHEMA.SYSTEM_USERS"
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

    // Stand in for what a service like clearance already owns: its own Hikari pool and Slick executor.
    val hikariConfig = new HikariConfig()
    hikariConfig.setPoolName("adopted-write")
    hikariConfig.setJdbcUrl(s"jdbc:hsqldb:mem:$name")
    hikariConfig.setDriverClassName("org.hsqldb.jdbc.JDBCDriver")
    hikariConfig.setUsername("sa")
    hikariConfig.setMaximumPoolSize(4)
    hikariConfig.setConnectionTestQuery("SELECT 1 FROM INFORMATION_SCHEMA.SYSTEM_USERS")
    val existingDataSource = new HikariDataSource(hikariConfig)
    val existingExecutor = AsyncExecutor("adopted-executor", 4, 4, 1000, 4, registerMbeans = true)
    val existingDb: JdbcBackend#Database = JdbcBackend.Database.forDataSource(existingDataSource, Some(4), existingExecutor)

    try {
      val configuration = DualPoolConfig.load(ConfigFactory.parseString(
        s"""db { url = "jdbc:hsqldb:mem:$name", driver = "org.hsqldb.jdbc.JDBCDriver", username = "sa", maxPoolSize = 4 }
           |readDb {
           |  poolName = "adopted-read"
           |  maxConnections = 2
           |  queueSize = 10
           |  validationQuery = "SELECT 1 FROM INFORMATION_SCHEMA.SYSTEM_USERS"
           |}""".stripMargin))

      val pool = DualPool.adoptingWritePool(
        profile, existingDb, configuration, identity,
        adoptedWriteDataSource = Some(existingDataSource),
        adoptedWriteExecutorName = Some("adopted-executor"),
        metricsRegistry = PrometheusPoolMetricsRegistry.forRegistry(registry))

      try {
        pool.writeDb should be theSameInstanceAs existingDb
        pool.isReadPoolIsolated shouldBe true

        // Both pools report, which is what makes read and write comparable on one dashboard.
        noException should be thrownBy pool.metrics.verifyRegistered()
        pool.metricsSnapshot(DatabaseRole.Write).poolName shouldBe "adopted-write"
        await(ping(pool.writeDb))
        pool.metricsSnapshot(DatabaseRole.Write).executorQueueDepth should be >= 0
      } finally pool.close()

      // The adopted pool survives close(): the service built it and the service closes it.
      existingDataSource.isClosed shouldBe false
      await(ping(existingDb))
    } finally {
      existingDb.close()
      existingDataSource.close()
    }
  }

  test("an adopted write pool with the read pool disabled gets metrics but no second pool") {
    // The state a service deploys first: readDb { enabled = false }. Reads still go to the write pool, so
    // behaviour is unchanged, but the write pool is now reported and there is a before-picture to compare to.
    val name = databaseName("metrics-only")
    val registry = new CollectorRegistry()

    val hikariConfig = new HikariConfig()
    hikariConfig.setPoolName("metrics-only-write")
    hikariConfig.setJdbcUrl(s"jdbc:hsqldb:mem:$name")
    hikariConfig.setDriverClassName("org.hsqldb.jdbc.JDBCDriver")
    hikariConfig.setUsername("sa")
    hikariConfig.setMaximumPoolSize(4)
    hikariConfig.setConnectionTestQuery("SELECT 1 FROM INFORMATION_SCHEMA.SYSTEM_USERS")
    val existingDataSource = new HikariDataSource(hikariConfig)
    val existingExecutor = AsyncExecutor("metrics-only-executor", 4, 4, 1000, 4, registerMbeans = true)
    val existingDb: JdbcBackend#Database = JdbcBackend.Database.forDataSource(existingDataSource, Some(4), existingExecutor)

    try {
      val configuration = DualPoolConfig.load(ConfigFactory.parseString(
        s"""db { url = "jdbc:hsqldb:mem:$name", driver = "org.hsqldb.jdbc.JDBCDriver", username = "sa", maxPoolSize = 4 }
           |readDb { enabled = false, maxConnections = 2 }""".stripMargin))
      configuration.read shouldBe None

      val pool = DualPool.adoptingWritePool(
        profile, existingDb, configuration, identity,
        adoptedWriteDataSource = Some(existingDataSource),
        adoptedWriteExecutorName = Some("metrics-only-executor"),
        metricsRegistry = PrometheusPoolMetricsRegistry.forRegistry(registry))

      try {
        pool.isReadPoolIsolated shouldBe false
        pool.readDb should be theSameInstanceAs existingDb

        await(ping(pool.writeDb))
        noException should be thrownBy pool.metrics.verifyRegistered()

        val writeMetrics = pool.metricsSnapshot(DatabaseRole.Write)
        writeMetrics.poolName shouldBe "metrics-only-write"
        // Queue depth is readable because the service's own executor registers its MBean.
        writeMetrics.executorQueueDepth shouldBe 0
        writeMetrics.totalConnections should be > 0
      } finally pool.close()

      existingDataSource.isClosed shouldBe false
    } finally {
      existingDb.close()
      existingDataSource.close()
    }
  }

  test("an adopted pool built with setters still reports, because the library starts it") {
    // Reproduces how clearance-service builds its write pool: `new HikariDataSource()` plus setters, which does
    // not start the pool until the first getConnection. HikariCP registers a pool with its Prometheus exporter
    // only when the pool starts, so before the library warmed it up this combination failed the service start
    // with "pool metrics are not fully registered".
    val name = databaseName("lazy-adopt")
    val registry = new CollectorRegistry()

    val existingDataSource = new HikariDataSource()
    existingDataSource.setPoolName("lazy-adopted-write")
    existingDataSource.setJdbcUrl(s"jdbc:hsqldb:mem:$name")
    existingDataSource.setDriverClassName("org.hsqldb.jdbc.JDBCDriver")
    existingDataSource.setUsername("sa")
    existingDataSource.setMaximumPoolSize(4)
    existingDataSource.setConnectionTestQuery("SELECT 1 FROM INFORMATION_SCHEMA.SYSTEM_USERS")
    existingDataSource.isRunning shouldBe false

    val existingExecutor = AsyncExecutor("lazy-adopted-executor", 4, 4, 1000, 4, registerMbeans = true)
    val existingDb: JdbcBackend#Database = JdbcBackend.Database.forDataSource(existingDataSource, Some(4), existingExecutor)

    try {
      val configuration = DualPoolConfig.load(ConfigFactory.parseString(
        s"""db {
           |  url = "jdbc:hsqldb:mem:$name"
           |  driver = "org.hsqldb.jdbc.JDBCDriver"
           |  username = "sa"
           |  maxPoolSize = 4
           |  preferredTestQuery = "SELECT 1 FROM INFORMATION_SCHEMA.SYSTEM_USERS"
           |}
           |readDb { poolName = "lazy-adopted-read", maxConnections = 2, queueSize = 10 }""".stripMargin))

      val pool = DualPool.adoptingWritePool(
        profile, existingDb, configuration, identity,
        adoptedWriteDataSource = Some(existingDataSource),
        adoptedWriteExecutorName = Some("lazy-adopted-executor"),
        metricsRegistry = PrometheusPoolMetricsRegistry.forRegistry(registry))

      try {
        pool.metricsSnapshot(DatabaseRole.Write).totalConnections should be > 0
        pool.metricsSnapshot(DatabaseRole.Read).poolName shouldBe "lazy-adopted-read"
      } finally pool.close()
    } finally {
      existingDb.close()
      existingDataSource.close()
    }
  }

  test("a read pool the library built must publish, even though an adopted one need not") {
    // The asymmetry is deliberate: the library controls a pool it built, so missing metrics there are a bug
    // worth failing the boot over. An adopted pool's lifecycle belongs to the service.
    val registry = new CollectorRegistry()
    val metrics = PrometheusPoolMetricsRegistry.forRegistry(registry)

    metrics.register(DatabaseRole.Write, PoolStatsSource.fixed("silent-adopted"), strict = false)
    noException should be thrownBy metrics.verifyRegistered()

    metrics.register(DatabaseRole.Read, PoolStatsSource.fixed("silent-owned"), strict = true)
    val failure = the[IllegalStateException] thrownBy metrics.verifyRegistered()
    failure.getMessage should include("pool 'silent-owned' (Read) is missing metric")
    failure.getMessage should not include "silent-adopted"
  }

  test("the read queue is bounded, so overload is refused rather than accumulated") {
    val configuration = dualPoolConfig(
      databaseName("overload"),
      "overload-write",
      Some(readSection("overload-read", maxConnections = 1, queueSize = 1)))

    withPool(DualPool(profile, configuration)) { pool =>
      val release = new java.util.concurrent.CountDownLatch(1)
      val blocker = hold(pool.readDb, release).recover { case _ => () }

      try {
        eventually() { pool.metricsSnapshot(DatabaseRole.Read).activeConnections == 1 } shouldBe true

        val flood = (1 to 50).map(_ => ping(pool.readDb))
        val rejected = eventually(10.seconds) {
          flood.count(_.value.exists(_.isFailure)) > 0
        }

        withClue("a bounded queue must reject once it is full rather than growing without limit: ") {
          rejected shouldBe true
        }
      } finally {
        release.countDown()
        scala.util.Try(await(blocker))
      }
    }
  }
}
