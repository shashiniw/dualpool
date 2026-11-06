package com.lab.dualpool.config

import scala.concurrent.duration.FiniteDuration

/**
 * Settings for a single pool. One instance per config section: `db`, `readDb`, `jobDb`.
 *
 * @param name     pool name, unique across pools. Used as the HikariCP pool name, the Slick executor name
 *                 and the value of the `pool` metric label, so it has to be stable.
 * @param readOnly when true, a write on this pool fails fast.
 * @param adopted  true when the service built this pool itself and handed it to the library. Adopted pools
 *                 are reported on and validated leniently, but never built and never closed by the library.
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
