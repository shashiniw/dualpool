package com.lab.dualpool

import com.lab.dualpool.config.DualPoolConfig
import com.typesafe.config.{Config, ConfigFactory}
import slick.jdbc.HsqldbProfile

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

/** Shared setup for the tests that need real pools against a real (in-memory) database. */
trait HsqldbFixture {

  protected val profile: HsqldbProfile = HsqldbProfile

  protected implicit val executionContext: ExecutionContext = ExecutionContext.global

  /** A fresh in-memory database per test, so pool names and MBeans never collide between tests. */
  protected def databaseName(suffix: String): String = s"dualpool-$suffix-${HsqldbFixture.counter.incrementAndGet()}"

  protected def config(name: String,
                       writePool: String,
                       readSection: Option[String] = None,
                       writeOverrides: String = ""): Config =
    ConfigFactory.parseString(
      s"""db {
         |  url = "jdbc:hsqldb:mem:$name"
         |  driver = "org.hsqldb.jdbc.JDBCDriver"
         |  username = "sa"
         |  password = ""
         |  poolName = "$writePool"
         |  validationQuery = "SELECT 1 FROM INFORMATION_SCHEMA.SYSTEM_USERS"
         |  $writeOverrides
         |}
         |${readSection.getOrElse("")}
         |""".stripMargin)

  protected def dualPoolConfig(name: String,
                               writePool: String,
                               readSection: Option[String] = None,
                               writeOverrides: String = ""): DualPoolConfig =
    DualPoolConfig.load(config(name, writePool, readSection, writeOverrides))

  protected def readSection(poolName: String, maxConnections: Int = 2, queueSize: Int = 10): String =
    s"""readDb {
       |  poolName = "$poolName"
       |  maxConnections = $maxConnections
       |  queueSize = $queueSize
       |  connectionTimeout = 2s
       |}""".stripMargin

  /** Runs the pool's own validation query, which is the cheapest thing that exercises connection + executor. */
  protected def ping(database: slick.jdbc.JdbcBackend#Database): Future[Unit] = {
    import profile.api._
    database.run(SimpleDBIO[Unit] { context =>
      val statement = context.connection.createStatement()
      try statement.execute("SELECT 1 FROM INFORMATION_SCHEMA.SYSTEM_USERS") finally statement.close()
    })
  }

  /** Occupies one executor thread and one connection until `release` is counted down. */
  protected def hold(database: slick.jdbc.JdbcBackend#Database, release: java.util.concurrent.CountDownLatch): Future[Unit] = {
    import profile.api._
    database.run(SimpleDBIO[Unit] { context =>
      val statement = context.connection.createStatement()
      try {
        statement.execute("SELECT 1 FROM INFORMATION_SCHEMA.SYSTEM_USERS")
        release.await(30, java.util.concurrent.TimeUnit.SECONDS)
        ()
      } finally statement.close()
    })
  }

  /** Polls until `condition` holds, so the tests do not depend on a fixed sleep. */
  protected def eventually(timeout: FiniteDuration = 5.seconds)(condition: => Boolean): Boolean = {
    val deadline = timeout.fromNow
    while (deadline.hasTimeLeft() && !condition) Thread.sleep(25)
    condition
  }

  protected def await[A](future: Future[A], timeout: FiniteDuration = 10.seconds): A = Await.result(future, timeout)

  protected def withPool[A](pool: DualPool)(body: DualPool => A): A =
    try body(pool) finally pool.close()
}

object HsqldbFixture {
  private val counter = new AtomicInteger(0)
}
