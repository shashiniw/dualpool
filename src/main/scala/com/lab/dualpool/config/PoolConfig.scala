package com.lab.dualpool.config

import com.lab.dualpool.DatabaseRole
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration.{DurationLong, FiniteDuration}

/**
 * Settings for one pool (proposal §4.3). One instance per config section: `db`, `readDb`, optional `jobDb`.
 *
 * @param name     HikariCP poolName; required and unique across pools so metrics are never conflated (§5.5).
 *                 This is also the value of the `pool` label on every metric and the name of the pool's
 *                 Slick executor MBean, so it has to be stable.
 * @param readOnly sets HikariCP readOnly: writes on this pool fail fast. Not in the proposal; opt-in, default false
 * @param adopted  true when the service already built this pool and handed it to the library. Adopted pools are
 *                 reported on and validated leniently, but never constructed and never closed by the library.
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
                            readOnly: Boolean = false,
                            minIdle: Option[Int] = None,
                            idleTimeout: Option[FiniteDuration] = None,
                            validationQuery: String = "SELECT 1",
                            adopted: Boolean = false)

/**
 * All pools of a service. `read` is None when the `readDb` section is absent or disabled, in which case reads
 * fall back to the write pool (proposal §4.2 "Config convention" and §5.7 backward compatibility).
 *
 * @param instances               how many instances (pods) of this service run, used by the sizing check (§5.3)
 * @param databaseConnectionLimit the database's `max_connections`, when the service knows it
 */
final case class DualPoolConfig(write: PoolConfig,
                                read: Option[PoolConfig],
                                background: Option[PoolConfig],
                                instances: Int = 1,
                                databaseConnectionLimit: Option[Int] = None) {

  /** Every pool actually configured, in a stable order. */
  def pools: Seq[(DatabaseRole, PoolConfig)] =
    Seq(Option(DatabaseRole.Write -> write), read.map(DatabaseRole.Read -> _), background.map(DatabaseRole.Background -> _)).flatten
}

object DualPoolConfig {
  val DefaultWritePath = "db"
  val DefaultReadPath = "readDb"
  val DefaultBackgroundPath = "jobDb"

  private val DefaultsPath = "dualpool.defaults"
  private val ReadDefaultsPath = "dualpool.read-defaults"
  private val InstancesPath = "dualpool.instances"
  private val ConnectionLimitPath = "dualpool.databaseConnectionLimit"

  /**
   * Parses the standard layout from `config`; absent optional sections become None.
   *
   * Legacy key names are accepted so a service can adopt the library without rewriting the `db` section it
   * already has: `maxPoolSize` for `maxConnections`, `initialPoolSize` for `minIdle`,
   * `acquireConnectionTimeout` for `connectionTimeout`, `idleConnectionTestPeriod` for `idleTimeout` and
   * `preferredTestQuery` for `validationQuery`.
   *
   * `readDb`/`jobDb` inherit `url`, `driver`, `username`, `password` and `validationQuery` from the write section
   * when they do not set them, so pointing a read pool at the same database is a three-line config change.
   * Sizing keys are deliberately not inherited: a read pool that silently copied the write pool's size would
   * double the service's connection count without anyone choosing that. A section with
   * `enabled = false` is treated as absent, which is what lets an environment switch the read pool on and off
   * without a code change.
   */
  def load(config: Config,
           writePath: String = DefaultWritePath,
           readPath: String = DefaultReadPath,
           backgroundPath: String = DefaultBackgroundPath): DualPoolConfig = {
    val root = config.withFallback(ConfigFactory.defaultReference())
    val defaults = root.getConfig(DefaultsPath)
    val readDefaults = root.getConfig(ReadDefaultsPath).withFallback(defaults)

    val write = parse(DatabaseRole.Write, requireSection(root, writePath), defaults, None)
    val read = enabledSection(root, readPath).map(parse(DatabaseRole.Read, _, readDefaults, Some(write)))
    val background = enabledSection(root, backgroundPath).map(parse(DatabaseRole.Background, _, defaults, Some(write)))

    DualPoolConfig(
      write = write,
      read = read,
      background = background,
      instances = if (root.hasPath(InstancesPath)) root.getInt(InstancesPath) else 1,
      databaseConnectionLimit = if (root.hasPath(ConnectionLimitPath)) Some(root.getInt(ConnectionLimitPath)) else None
    )
  }

  private def requireSection(root: Config, path: String): Config =
    if (root.hasPath(path)) root.getConfig(path)
    else throw new IllegalArgumentException(s"dualpool: required config section '$path' is missing")

  /** A section that is present and not explicitly disabled. */
  private def enabledSection(root: Config, path: String): Option[Config] =
    if (!root.hasPath(path)) None
    else {
      val section = root.getConfig(path)
      if (section.hasPath("enabled") && !section.getBoolean("enabled")) None else Some(section)
    }

  private def parse(role: DatabaseRole, section: Config, defaults: Config, parent: Option[PoolConfig]): PoolConfig = {
    val maxConnections = int(section, "maxConnections", "maxPoolSize").getOrElse(defaults.getInt("maxConnections"))
    PoolConfig(
      name = string(section, "poolName").getOrElse(role.defaultPoolName),
      url = string(section, "url").orElse(parent.map(_.url))
        .getOrElse(throw new IllegalArgumentException(s"dualpool: '${role.configSection}.url' is missing")),
      driver = string(section, "driver", "driverClassName").orElse(parent.map(_.driver)).getOrElse(""),
      username = string(section, "username", "user").orElse(parent.flatMap(_.username)),
      password = string(section, "password").orElse(parent.flatMap(_.password)),
      // numThreads defaults to maxConnections: a thread that cannot get a connection is a thread that only
      // makes the queue harder to read (§5.4, and the startup check in PoolConfigValidator).
      numThreads = int(section, "numThreads").getOrElse(maxConnections),
      maxConnections = maxConnections,
      queueSize = int(section, "queueSize").getOrElse(defaults.getInt("queueSize")),
      connectionTimeout = duration(section, "connectionTimeout", "acquireConnectionTimeout")
        .getOrElse(defaults.getDuration("connectionTimeout").toMillis.millis),
      readOnly = bool(section, "readOnly").getOrElse(false),
      minIdle = int(section, "minIdle", "initialPoolSize"),
      idleTimeout = duration(section, "idleTimeout", "idleConnectionTestPeriod"),
      // Inherited like url and driver: the test query is a property of the database, not of the pool.
      validationQuery = string(section, "validationQuery", "preferredTestQuery")
        .orElse(parent.map(_.validationQuery)).getOrElse("SELECT 1"),
      adopted = false
    )
  }

  private def firstPath(c: Config, keys: Seq[String]): Option[String] = keys.find(c.hasPath)
  private def string(c: Config, keys: String*): Option[String] = firstPath(c, keys).map(c.getString).filter(_.nonEmpty)
  private def int(c: Config, keys: String*): Option[Int] = firstPath(c, keys).map(c.getInt)
  private def bool(c: Config, keys: String*): Option[Boolean] = firstPath(c, keys).map(c.getBoolean)

  /** Accepts both `2s` and a bare number of milliseconds, which is how the legacy keys were written. */
  private def duration(c: Config, keys: String*): Option[FiniteDuration] =
    firstPath(c, keys).map(c.getDuration(_).toMillis.millis)
}
