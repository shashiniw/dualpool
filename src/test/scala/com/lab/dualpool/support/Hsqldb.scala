package com.lab.dualpool.support

/** The in-memory database the tests run against. */
object Hsqldb {

  val Driver = "org.hsqldb.jdbc.JDBCDriver"
  val Username = "sa"

  /** HSQLDB has no bare `SELECT 1`, so every query here goes through a table that always exists. */
  val TestQuery = "SELECT 1 FROM INFORMATION_SCHEMA.SYSTEM_USERS"

  def url(databaseName: String): String = s"jdbc:hsqldb:mem:$databaseName"
}
