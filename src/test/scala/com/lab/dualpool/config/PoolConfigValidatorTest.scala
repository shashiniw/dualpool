package com.lab.dualpool.config

import com.lab.dualpool.config.PoolConfigValidator.{Error, Warning}
import org.scalatest.LoneElement
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class PoolConfigValidatorTest extends AnyFunSuite with Matchers with LoneElement {

  private def pool(name: String,
                   threads: Int = 4,
                   connections: Int = 4,
                   queue: Int = 100,
                   adopted: Boolean = false): PoolConfig =
    PoolConfig(
      name = name,
      url = "jdbc:hsqldb:mem:validator",
      driver = "org.hsqldb.jdbc.JDBCDriver",
      username = None,
      password = None,
      numThreads = threads,
      maxConnections = connections,
      queueSize = queue,
      connectionTimeout = 2.seconds,
      adopted = adopted)

  private def errorsOf(config: DualPoolConfig, instances: Int = 1, limit: Option[Int] = None): Seq[String] =
    PoolConfigValidator.validate(config, instances, limit).collect { case Error(message) => message }

  test("numThreads different from maxConnections is reported") {
    val config = DualPoolConfig(pool("write", threads = 20, connections = 6), None, None)
    errorsOf(config).loneElement should include("numThreads (20) != maxConnections (6)")
  }

  test("an adopted pool is warned about rather than refused, so adoption is not blocked by existing wiring") {
    // A service may run a 20-thread executor against a 6-connection Hikari pool. The library has to report that
    // without refusing to start, because it did not build that pool and cannot fix it.
    val config = DualPoolConfig(pool("write", threads = 20, connections = 6, adopted = true), Some(pool("read")), None)
    val problems = PoolConfigValidator.validate(config)

    problems.collect { case e: Error => e } shouldBe empty
    problems.collect { case Warning(message) => message }.loneElement should include("numThreads (20) != maxConnections (6)")
  }

  test("duplicate pool names are rejected") {
    val config = DualPoolConfig(pool("same"), Some(pool("same")), None)
    errorsOf(config).loneElement should include("pool name 'same' is used by more than one pool")
  }

  test("an empty pool name is rejected") {
    val config = DualPoolConfig(pool("  "), None, None)
    errorsOf(config) should contain("db: poolName must not be empty")
  }

  test("an unbounded queue is rejected") {
    val config = DualPoolConfig(pool("write", queue = 0), None, None)
    errorsOf(config).loneElement should include("queueSize must be > 0")
  }

  test("total connections above the database limit fails fast") {
    // 6 pods x (4 write + 4 read) = 48 connections against a limit of 40.
    val config = DualPoolConfig(pool("write"), Some(pool("read")), None)
    errorsOf(config, instances = 6, limit = Some(40)).loneElement should
      include("6 instance(s) x 8 connections = 48 exceeds the database limit of 40")
  }

  test("total connections near the database limit warns but still starts") {
    val config = DualPoolConfig(pool("write"), Some(pool("read")), None)
    val problems = PoolConfigValidator.validate(config, instanceCount = 6, databaseConnectionLimit = Some(50))

    problems.collect { case e: Error => e } shouldBe empty
    problems.collect { case Warning(message) => message }.loneElement should include("above 80% of the database limit")
  }

  test("validateOrFail throws on an error and returns quietly on a clean config") {
    val bad = DualPoolConfig(pool("write"), Some(pool("write")), None)
    a[IllegalStateException] should be thrownBy PoolConfigValidator.validateOrFail(bad)

    val good = DualPoolConfig(pool("write"), Some(pool("read")), None)
    noException should be thrownBy PoolConfigValidator.validateOrFail(good, 6, Some(100))
  }
}
