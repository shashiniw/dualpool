package com.lab.dualpool.config

import scala.concurrent.duration.FiniteDuration

/**
 * Settings for one pool (proposal §4.3). One instance per config section: `db`, `readDb`, optional `jobDb`.
 *
 * @param name     HikariCP poolName; required and unique across pools so metrics are never conflated (§5.5)
 * @param readOnly sets HikariCP readOnly: writes on this pool fail fast. Not in the proposal; opt-in, default false
 */
final case class PoolConfig(name: String,
                            url: String,
                            driver: String,
                            username: Option[String],
                            password: Option[String],
                            numThreads: Int,
                            maxConnections: Int,
                            queueSize: Int,
                            connectionTimeout: FiniteDuration,
                            readOnly: Boolean = false)

/**
 * All pools of a service. `read` is None when the `readDb` section is absent, in which case reads
 * fall back to the write pool (proposal §4.2 "Config convention" and §5.7 backward compatibility).
 */
final case class DualPoolConfig(write: PoolConfig,
                                read: Option[PoolConfig],
                                background: Option[PoolConfig])

object DualPoolConfig {
  val DefaultWritePath = "db"
  val DefaultReadPath = "readDb"
  val DefaultBackgroundPath = "jobDb"

  /** Parses the standard layout from `config`; absent optional sections become None. */
  def load(config: com.typesafe.config.Config,
           writePath: String = DefaultWritePath,
           readPath: String = DefaultReadPath,
           backgroundPath: String = DefaultBackgroundPath): DualPoolConfig = ???
}
