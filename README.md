# dualpool

Separate read and write connection pools for Slick services.

A service with one connection pool has one failure mode: a slow or stalled write holds every connection, and
reads start failing with it. dualpool gives each workload its own pool — its own HikariCP connections, its own
Slick executor with a bounded queue, and its own metrics — so the two can no longer starve each other.

Adding it is a config change. Existing code keeps working: until a `readDb` section exists, `readDb` *is* the
write pool, the same object.

---

## Install

```scala
libraryDependencies += "com.lab" %% "dualpool" % "0.1.0-SNAPSHOT"
```

Needs Scala 2.13 and a JDK 11 or newer (HikariCP 7.1 will not load on 8).

To build it locally:

```bash
sbt test          # 23 tests, including fault injection against an in-memory HSQLDB
sbt publishLocal  # com.lab:dualpool_2.13:0.1.0-SNAPSHOT to ~/.ivy2/local
```

---

## Use it

### 1. Build the pools at startup

```scala
import com.lab.dualpool.DualPool
import slick.jdbc.PostgresProfile

val pools = DualPool.fromConfig(PostgresProfile, config)   // service startup hook
...
pools.close()                                              // service shutdown hook
```

Nothing is created until `fromConfig` is called, and nothing is cleaned up unless you `close()`. Both belong in
the service's own lifecycle hooks, not in a field initializer.

### 2. Point your DAOs at the right pool

```scala
class CustomerDao(pools: DualPool) {
  def find(id: Long)  = pools.readDb.run(customers.filter(_.id === id).result)   // read pool
  def save(c: Customer) = pools.writeDb.run(customers.insertOrUpdate(c))         // write pool
}
```

| Accessor | Pool | Falls back to |
|---|---|---|
| `writeDb` | `db` section | — |
| `readDb` | `readDb` section | `writeDb` when `readDb` is absent or disabled |
| `backgroundDb` | `jobDb` section | `writeDb` when `jobDb` is absent |

Migrate one DAO at a time. Anything you have not touched keeps using `writeDb`.

### 3. Configure

Add a `readDb` section next to the `db` section you already have:

```hocon
readDb {
  enabled = false        # flip per environment; off means reads stay on `db`
  poolName = "ServiceReadDS"
  url = ${db.url}        # the same database (isolation only), or a read replica
  maxConnections = 3     # numThreads is set to match
  queueSize = 200
  connectionTimeout = 2s
  readOnly = false       # true makes a write on this pool fail fast
}

dualpool {
  instances = 6                  # pods, for the startup sizing check
  databaseConnectionLimit = 100   # the database's max_connections
}
```

`readDb` inherits `url`, `driver`, `username`, `password` and `validationQuery` from `db` when it does not set
them, so pointing it at the same database is a three-line change. Sizing keys are never inherited — a read pool
that silently copied the write pool's size would double the service's connection count without anyone choosing
that.

Defaults for every key live in [`reference.conf`](src/main/resources/reference.conf). The legacy spellings
`maxPoolSize`, `initialPoolSize`, `acquireConnectionTimeout`, `idleConnectionTestPeriod`, `preferredTestQuery`,
`driverClassName` and `user` are accepted too, so an existing `db` section parses unchanged.

### Already have a write pool?

Keep it. dualpool builds only the read pool and leaves yours alone — it is never reconfigured and never closed:

```scala
val pools = DualPool.adoptingWritePool(
  profile, existingDb, DualPoolConfig.load(config),
  adoptedWriteDataSource   = Some(existingHikariDataSource),  // optional: lets the write pool report too
  adoptedWriteExecutorName = Some("AsyncExecutor.default"))   // optional: and report its queue depth

val readDb = pools.readDb   // == existingDb while readDb is absent or disabled
```

Passing the DataSource and executor name changes nothing about how your pool behaves; it only means both pools
show up on the same dashboard, which is what makes them comparable. To report refused work for your pool as
well, wrap its executor with `DualPool.countingRejections(executor, counter)` and pass the same counter as
`adoptedWriteRejections`.

---

## What you get on a dashboard

Every metric carries a `pool="<poolName>"` label, so read and write are the same panel split by one label.

| Metric | From |
|---|---|
| `slick_executor_queue_size` | dualpool — **the saturation signal to alert on** |
| `slick_executor_queue_capacity`, `slick_executor_active_threads`, `slick_executor_rejected_total` | dualpool |
| `dualpool_connection_acquired_seconds` | dualpool — a histogram, so a p95 can be computed across pods |
| `hikaricp_active_connections`, `hikaricp_idle_connections`, `hikaricp_pending_threads`, `hikaricp_connections` | HikariCP |
| `hikaricp_connection_acquired_nanos`, `hikaricp_connection_timeout_total` | HikariCP |

Alert on queue depth and acquire time. `hikaricp_pending_threads` reads zero while the executor queue is deep,
because work waits for a thread before it ever asks for a connection.

Metrics are registered when a pool is built, not on its first query, so a pool nobody has used yet still
reports — a metric that is simply absent reads as healthy.

`io.prometheus:simpleclient` is a `Provided` dependency. Without it the pools still work and
`PoolMetricsRegistry.default` falls back to a no-op that logs a warning. With it, pass your own registry:

```scala
DualPool(profile, config, metricsRegistry = PrometheusPoolMetricsRegistry.forRegistry(registry))
```

Use `forRegistry` rather than constructing a registry yourself: a Prometheus registry accepts a given collector
only once, so a second instance would silently report nothing.

---

## Safety checks at startup

`DualPool.apply` refuses to start on a configuration that cannot work, rather than failing on a user's request:

- pool names are present and unique, or metrics and executor MBeans would be conflated;
- `numThreads == maxConnections`, since surplus threads block inside Slick waiting for a connection and hide
  saturation from the queue metric;
- `queueSize > 0`, so overload is rejected rather than accumulated;
- `instances * sum(maxConnections)` fits `databaseConnectionLimit` (a warning above 80% of it);
- every pool answers its validation query, which proves the credentials, the executor's threads and the MBean
  behind `slick_executor_queue_size`.

A pool you handed over with `adoptingWritePool` is warned about rather than refused, because the library did not
build it and cannot fix it.

The effective size of every pool is logged at startup, so nobody has to infer it from the config files.

---

## Layout

```
com/lab/dualpool/
  DualPool.scala          the entry point: writeDb, readDb, backgroundDb, close()
  DatabaseRole.scala      Write / Read / Background, and the config section each maps to
  config/                 PoolConfig, DualPoolConfig (parsing), PoolConfigValidator (startup checks)
  metrics/                the metric contract, live readings, and the Prometheus exporter
  internal/               pool construction; not part of the public API
```

`src/test` mirrors it, with the shared HSQLDB setup in `support/`.
