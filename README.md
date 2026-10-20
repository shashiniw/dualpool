# dualpool

Read/write connection pool isolation for Slick services, as described in the
*Read/Write Connection Pool Isolation* proposal (13 Oct 2026).

Each workload gets its own HikariCP pool, its own Slick executor and bounded queue, and its own metrics.
A stalled write can no longer starve reads of connections.

## Build

```bash
sdk use java 25.0.3-tem   # any JDK 11+; HikariCP 7.1 will not load on 8
sbt test                  # 26 tests, including fault injection against HSQLDB
sbt publishLocal          # com.lab:dualpool_2.13:0.1.0-SNAPSHOT to ~/.ivy2/local
```

Only Scala 2.13.18 has been built. `crossScalaVersions` still lists 3.3.8 but those artifacts were never
resolvable here; run `sbt +compile` where they are.

## Use it from a service

```scala
libraryDependencies += "com.lab" %% "dualpool" % "0.1.0-SNAPSHOT"
```

Add a `readDb` section next to the existing `db` section. Absent, or `enabled = false`, and reads use `db`.
`url`, `driver`, credentials and the test query are inherited from `db` when not set here; sizing keys never are.

```hocon
readDb {
  enabled = false           # flip per environment; off means reads stay on `db`
  poolName = "ServiceReadDS"
  url = ${db.url}           # same primary (pure isolation) or a read replica
  maxConnections = 3        # numThreads is set to match
  queueSize = 200
  connectionTimeout = 2s
  readOnly = false          # true makes a write on this pool fail fast
}

dualpool {
  instances = 6                    # pods, for the startup sizing check
  databaseConnectionLimit = 100    # the database's max_connections
}
```

A service that already owns a write pool keeps it and adds only the read pool:

```scala
val dualPool = DualPool.adoptingWritePool(
  profile, existingDb, DualPoolConfig.load(config),
  adoptedWriteDataSource   = Some(existingHikariDataSource),  // optional: lets the write pool report too
  adoptedWriteExecutorName = Some("AsyncExecutor.default"))   // optional: its queue depth
val readDb = dualPool.readDb   // == existingDb while readDb is absent or disabled
```

Greenfield services use `DualPool.fromConfig(profile, config)`. Build the pools from the service's startup hook
and `close()` them from its shutdown hook — nothing is created as a field-initializer side effect.

## API

| Responsibility | Type / method |
|---|---|
| Config convention | `DualPoolConfig.load`, `PoolConfig`, `reference.conf` defaults |
| Startup safety | `PoolConfigValidator.validate` / `validateOrFail` |
| Pool construction | `DualPool.apply` / `fromConfig` / `adoptingWritePool`, internal `PoolFactory` |
| Lifecycle | `DualPool.close()` (idempotent; never closes an adopted pool) |
| Observability | `PrometheusPoolMetricsRegistry.forRegistry`, `PoolStatsSource`, `PoolMetrics` |
| Optional job pool | `DatabaseRole.Background`, `backgroundDb` (`jobDb` section) |

`executorDecorator` wraps each pool's executor, for MDC propagation and the like.

## Metrics

Per pool, labelled `pool="<poolName>"`:

| Metric | Source |
|---|---|
| `slick_executor_queue_size` | this library — **the saturation signal to alert on** |
| `slick_executor_queue_capacity`, `slick_executor_active_threads` | this library |
| `hikaricp_active_connections`, `hikaricp_idle_connections`, `hikaricp_pending_threads`, `hikaricp_connections` | HikariCP's Prometheus exporter |
| `hikaricp_connection_acquired_nanos`, `hikaricp_connection_timeout_total` | HikariCP's Prometheus exporter |

`hikaricp_pending_threads` reads zero while the executor queue is deep, because work waits for a thread before
it ever asks for a connection. Alert on queue depth and acquire time.

`io.prometheus:simpleclient` is a `Provided` dependency: without it the pools still work and
`PoolMetricsRegistry.default` falls back to a no-op that logs a warning. Use
`PrometheusPoolMetricsRegistry.forRegistry(registry)` rather than constructing one — a Prometheus registry
accepts a given collector only once, so a second instance would silently report nothing.

## What the tests cover

`sbt test` — config parsing including legacy key names, read-pool fallback, sizing validation, close
idempotence, adopted pools surviving close, startup probe failing at construction rather than first query,
the metric contract (identical metric set per pool, and a pool that has served no query still registered),
a bounded queue refusing overload, and the fault-injection case: a saturated write pool leaves read latency
and read-pool metrics untouched.
