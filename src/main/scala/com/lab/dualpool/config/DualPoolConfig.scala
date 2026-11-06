package com.lab.dualpool.config

import com.lab.dualpool.DatabaseRole
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration.{DurationLong, FiniteDuration}

/**
 * Every pool of a service. `read` is None when the `readDb` section is absent or disabled, in which case
 * reads fall back to the write pool.
 *
 * @param instances               how many instances (pods) of this service run; used by the sizing check
 * @param databaseConnectionLimit the database's `max_connections`, when the service knows it
 */
final case class DualPoolConfig(write: PoolConfig,
                                read: Option[PoolConfig],
                                background: Option[PoolConfig],
                                instances: Int = 1,
                                databaseConnectionLimit: Option[Int] = None) {

  /** Every pool actually configured, in a stable order. */
  def pools: Seq[(DatabaseRole, PoolConfig)] =
    Seq(
      Some(DatabaseRole.Write -> write),
      read.map(DatabaseRole.Read -> _),
      background.map(DatabaseRole.Background -> _)
    ).flatten
}

object DualPoolConfig {

  val DefaultWritePath = "db"
  val DefaultReadPath = "readDb"
  val DefaultBackgroundPath = "jobDb"

  private val DefaultsPath = "dualpool.defaults"
  private val ReadDefaultsPath = "dualpool.read-defaults"
  private val InstancesPath = "dualpool.instances"
  private val ConnectionLimitPath = "dualpool.databaseConnectionLimit"

  /** Legacy key names accepted alongside the current ones, so an existing `db` section parses unchanged. */
  private val LegacyKeys = Map(
    "maxConnections" -> "maxPoolSize",
    "minIdle" -> "initialPoolSize",
    "connectionTimeout" -> "acquireConnectionTimeout",
    "idleTimeout" -> "idleConnectionTestPeriod",
    "validationQuery" -> "preferredTestQuery",
    "driver" -> "driverClassName",
    "username" -> "user")

  /**
   * Parses the standard layout from `config`. Missing optional sections become None, as does a section with
   * `enabled = false`, which is how an environment switches the read pool on and off without a code change.
   *
   * `readDb` and `jobDb` inherit `url`, `driver`, `username`, `password` and `validationQuery` from the write
   * section when they do not set them. Sizing keys are never inherited: a read pool that silently copied the
   * write pool's size would double the service's connection count without anyone choosing that.
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
      databaseConnectionLimit = if (root.hasPath(ConnectionLimitPath)) Some(root.getInt(ConnectionLimitPath)) else None)
  }

  private def parse(role: DatabaseRole, section: Config, defaults: Config, parent: Option[PoolConfig]): PoolConfig = {
    val reader = new SectionReader(section)
    val maxConnections = reader.int("maxConnections").getOrElse(defaults.getInt("maxConnections"))

    PoolConfig(
      name = reader.string("poolName").getOrElse(role.defaultPoolName),
      url = reader.string("url").orElse(parent.map(_.url))
        .getOrElse(throw new IllegalArgumentException(s"dualpool: '${role.configSection}.url' is missing")),
      driver = reader.string("driver").orElse(parent.map(_.driver)).getOrElse(""),
      username = reader.string("username").orElse(parent.flatMap(_.username)),
      password = reader.string("password").orElse(parent.flatMap(_.password)),
      // numThreads defaults to maxConnections: a thread that cannot get a connection only makes the queue
      // metric harder to read.
      numThreads = reader.int("numThreads").getOrElse(maxConnections),
      maxConnections = maxConnections,
      queueSize = reader.int("queueSize").getOrElse(defaults.getInt("queueSize")),
      connectionTimeout = reader.duration("connectionTimeout")
        .getOrElse(defaults.getDuration("connectionTimeout").toMillis.millis),
      readOnly = reader.boolean("readOnly").getOrElse(false),
      minIdle = reader.int("minIdle"),
      idleTimeout = reader.duration("idleTimeout"),
      // Inherited like url and driver: the test query is a property of the database, not of the pool.
      validationQuery = reader.string("validationQuery").orElse(parent.map(_.validationQuery)).getOrElse("SELECT 1"))
  }

  private def requireSection(root: Config, path: String): Config =
    if (root.hasPath(path)) root.getConfig(path)
    else throw new IllegalArgumentException(s"dualpool: required config section '$path' is missing")

  /** A section that is present and not explicitly disabled. */
  private def enabledSection(root: Config, path: String): Option[Config] =
    Option(path).filter(root.hasPath).map(root.getConfig).filter { section =>
      !section.hasPath("enabled") || section.getBoolean("enabled")
    }

  /** Reads one key, falling back to its legacy name. */
  private final class SectionReader(section: Config) {

    def string(key: String): Option[String] = read(key).map(section.getString).filter(_.nonEmpty)

    def int(key: String): Option[Int] = read(key).map(section.getInt)

    def boolean(key: String): Option[Boolean] = read(key).map(section.getBoolean)

    /** Accepts both `2s` and a bare number of milliseconds, which is how the legacy keys were written. */
    def duration(key: String): Option[FiniteDuration] = read(key).map(section.getDuration(_).toMillis.millis)

    private def read(key: String): Option[String] =
      (key +: LegacyKeys.get(key).toSeq).find(section.hasPath)
  }
}
