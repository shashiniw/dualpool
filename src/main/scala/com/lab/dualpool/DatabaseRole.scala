package com.lab.dualpool

/** Which workload a pool serves. */
sealed trait DatabaseRole { def configSection: String }

object DatabaseRole {
  case object Write extends DatabaseRole { val configSection = "db" }
  case object Read extends DatabaseRole { val configSection = "readDb" }
  /** Optional third pool for batch/scheduled jobs (proposal §5.6). */
  case object Background extends DatabaseRole { val configSection = "jobDb" }
}
