# dualpool

Read/write connection pool isolation for Slick services, as described in the
*Read/Write Connection Pool Isolation* proposal (13 Oct 2026).

Each workload gets its own HikariCP pool, its own Slick executor and bounded queue, and its own metrics.
A stalled write can no longer starve reads of connections.

> **Status: skeleton.** The public API and package layout follow the proposal, but method bodies are `???`.
> It compiles and publishes, so services can wire against it. Calling any stub throws `NotImplementedError`.

## Build

```bash
export JAVA_HOME=~/.sdkman/candidates/java/25.0.3-tem
sbt test          # compiles (Scala 2.13.18) and runs the pending test skeletons
sbt publishLocal  # publishes com.lab:dualpool_2.13:0.1.0-SNAPSHOT to ~/.ivy2/local
```

If sbt hangs on `waiting for lock on ~/.sbt/boot/sbt.boot.lock`, another sbt instance holds the boot lock.
Close it, or pass `-Dsbt.boot.lock=false`.

Cross-building: `crossScalaVersions` lists 2.13.18 and 3.3.8, but only **2.13 has been built**. Scala 3 artifacts
could not be resolved in the environment used to write this. Run `sbt +compile` where they are available.

## Use it from a service

Add the dependency (the `%%` picks `dualpool_2.13`):

```scala
libraryDependencies += "com.lab" %% "dualpool" % "0.1.0-SNAPSHOT"
```

Add the optional `readDb` section next to the existing `db` section. If it is absent, reads use `db`.

```hocon
readDb {
  url = ${db.url}        # same primary (pure isolation) or a read replica
  numThreads = 10
  maxConnections = 10
  queueSize = 200
  connectionTimeout = 2s
  readOnly = true        # optional, default false: writes on this pool fail fast
}
```

Each pool's HikariCP `poolName` is required and must be unique (proposal 5.5). `readOnly` is an addition
to the proposal's convention: keep it off unless the DAOs on that pool are confirmed pure reads.

Services that already own a write pool keep it and add only the read pool:

```scala
val dualPool = DualPool.adoptingWritePool(profile, existingDb, DualPoolConfig.load(config))
val readDb   = dualPool.readDb   // == existingDb when readDb is not configured
```

Greenfield services can use `DualPool.fromConfig(profile, config)` to build both pools.
Then point read-only DAOs at `readDb` and leave everything else on `db`.

## API (proposal section 4.2)

| Responsibility | Type / method |
|---|---|
| Config convention | `DualPoolConfig.load`, `PoolConfig`, `reference.conf` defaults |
| Startup safety | `PoolConfigValidator.validate` / `validateOrFail` |
| Pool construction | `DualPool.apply` / `fromConfig` / `adoptingWritePool`, internal `PoolFactory` |
| Lifecycle | `DualPool.close()` (explicit, no field-initializer side effects) |
| Observability | `PoolMetricsRegistry.register` / `verifyRegistered` / `snapshot`, `PoolMetrics` |
| Optional job pool | `DatabaseRole.Background`, `backgroundDb` (`jobDb` section) |

`executorDecorator` lets a service wrap each executor.

## Tests to implement (proposal sections 5.5 and 7.2)

`src/test/scala/com/lab/dualpool/` holds pending tests for: identical metric set per pool,
registration of a pool that has not yet served a query, write saturation not affecting the read pool,
read fallback when `readDb` is absent, and sizing validation.

