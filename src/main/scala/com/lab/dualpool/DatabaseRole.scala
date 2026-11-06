package com.lab.dualpool

/** Which workload a pool serves. */
sealed trait DatabaseRole {

  /** Config section this role is read from. */
  def configSection: String

  /** Pool name used when the section does not set `poolName`. Also the `pool` metric label. */
  def defaultPoolName: String
}

object DatabaseRole {

  case object Write extends DatabaseRole {
    val configSection = "db"
    val defaultPoolName = "write"
  }

  case object Read extends DatabaseRole {
    val configSection = "readDb"
    val defaultPoolName = "read"
  }

  /** Optional third pool for batch and scheduled jobs. */
  case object Background extends DatabaseRole {
    val configSection = "jobDb"
    val defaultPoolName = "background"
  }

  val values: Seq[DatabaseRole] = Seq(Write, Read, Background)
}
