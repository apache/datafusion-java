# DataFusion Spark Connector

This module (`datafusion-java-spark`) lets you expose a [DataFusion
`TableProvider`](https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html)
written in Rust as an [Apache Spark DataSource
V2](https://spark.apache.org/docs/latest/sql-data-sources.html) table. If you
have data that DataFusion can already read — an in-house file format, a custom
catalog, a remote service — this connector is the bridge that makes
`spark.read.format(...)` work against it, with predicate pushdown, column
pruning, and partitioned parallel reads.

You write two small pieces (a Rust function and a Java class); the connector
supplies everything else.

## How it fits together

Two layers, one of which already exists:

```
 your bridge (you write this)          this module (already written)
+--------------------------------+   +----------------------------------+
| cdylib on datafusion-spark-    |   | Scala/Java DSv2 plumbing         |
| bridge (spark/bridge SDK):     |   |   (spark/src) schema inference,  |
|   your TableProvider + one     |<--|   pushdown, task planning,       |
|   export_bridge! invocation;   |-->|   shared-scan cache              |
|   the SDK supplies widening,   |   |                                  |
|   session, filters, planning,  |   | (pure JVM — all native code      |
|   partition streams            |   |  ships inside YOUR jar)          |
+--------------------------------+   +----------------------------------+
                                                     |
                                                     v
                                       spark.read.format("...").load()
```

The only things that cross between the JVM and your cdylib are opaque
`byte[]` blobs that *you* define (options and per-partition payloads — the
connector never inspects them) going in, and Arrow C streams coming back.
Everything DataFusion-side (planning, filter application, execution) happens
inside your bridge's native library. There is no DataFusion session on the
JVM side at all, and no `FFI_TableProvider` boundary anywhere — your
concrete provider is linked into the same cdylib as the scan machinery.

## Getting started: generate a bridge

Don't hand-assemble the pieces below — stamp them out:

```bash
python3 spark/scaffold/new_bridge.py --name acme --package com.example.acme
```

generates a standalone project (Rust cdylib with a working demo provider,
the four Java classes, service registration, shaded-jar pom with the cdylib
bundled, pyspark smoke test, README with the build commands). Replace the
demo `MemTable` in its `native/src/lib.rs` and you have a connector. The
sections below explain what each generated piece is for.

## What you implement

| # | Piece | Language | Contract lives at | Working example |
|---|-------|----------|-------------------|-----------------|
| 1 | A provider builder + one `export_bridge!` invocation | Rust | [`bridge/src/lib.rs`](bridge/src/lib.rs) (macro rustdoc) | [`examples/native/src/lib.rs`](../examples/native/src/lib.rs) |
| 2 | A `BridgeProviderFactory` implementation (one required method) + the JNI/backend boilerplate | Java | [`src/main/java/io/datafusion/spark/BridgeProviderFactory.java`](src/main/java/io/datafusion/spark/BridgeProviderFactory.java) | [`examples/.../ExampleBridgeProviderFactory.java`](../examples/src/main/java/org/apache/datafusion/examples/ExampleBridgeProviderFactory.java) |
| 3 | (optional) A `DatafusionSource` subclass giving your source a short name | Scala/Java | [`src/main/scala/io/datafusion/spark/DatafusionSource.scala`](src/main/scala/io/datafusion/spark/DatafusionSource.scala) | see "Wiring it into Spark" below |

An end-to-end runnable version of all three — in-memory table, factory, and a
PySpark script that scans, filters, and projects it — lives under
[`examples/python/`](../examples/python/).

### 1. The Rust side

Depend on the [`datafusion-spark-bridge`](bridge/) SDK crate and let it
generate the JNI surface; you supply one builder turning your option /
partition bytes into a concrete `TableProvider`:

```rust
use std::sync::Arc;
use datafusion_spark_bridge::datafusion::catalog::TableProvider;
use datafusion_spark_bridge::{export_bridge, BridgeContext, JniResult};

fn build_provider(
    ctx: &BridgeContext,
    options: &[u8],
    partition: &[u8],
) -> JniResult<Arc<dyn TableProvider>> {
    let opts = MyOptions::decode(options)?;
    Ok(ctx.block_on(MyProvider::connect(opts, partition))?)
}

export_bridge! {
    // Underscore-mangled name of YOUR Java class declaring the native
    // methods (dots -> underscores). Per-bridge names let several bridges
    // coexist in one Spark JVM.
    jni_class: "com_example_mybridge_BridgeNative",
    build_provider: build_provider,
}
```

The macro's rustdoc lists the exact `static native` method set the named
Java class must declare; your factory routes the connector to it by
overriding `scanBackend()` (see section 2). One cdylib total: your provider
and the SDK's scan machinery are the same library, so there is no provider
hand-off across a binary boundary and no `datafusion-ffi` anywhere. The
builder receives empty partition bytes for the driver-side schema probe —
schema must not depend on per-partition state.

[`examples/native/src/lib.rs`](../examples/native/src/lib.rs)
is a complete, commented version of this for a `MemTable`.

### 2. The Java factory

`BridgeProviderFactory` is the contract between Spark and your bridge. It
must have a no-arg constructor (executors instantiate it reflectively by
class name). The single required method is `scanBackend()` — Spark options
are encoded with `OptionsCodec` by default (decode them in Rust via
`datafusion_spark_bridge::options::decode_options`), and `listPartitions`
defaults to one whole-dataset partition:

```java
public final class MyBridgeProviderFactory implements BridgeProviderFactory {

    @Override
    public ScanBackend scanBackend() {
        return new MyBridgeBackend(); // six one-line delegations to BridgeNative
    }
}

/** Declares the native methods generated by export_bridge! and loads the cdylib. */
final class BridgeNative {
    static {
        NativeLibraryLoader.load(BridgeNative.class, "com/example/mybridge", "my_bridge");
    }
    static native byte[] providerSchemaIpc(byte[] options, byte[] partition);
    static native long createScan(byte[] options, byte[] partition,
        int targetPartitions, int batchSize, String[] optionKeys,
        String[] optionValues, String[] projectionColumns, byte[][] filterProtos);
    static native int partitionCount(long scanHandle);
    static native void executeStreamPartition(long scanHandle, int partition, long ffiStreamAddr);
    static native void executeStream(long scanHandle, long ffiStreamAddr);
    static native void closeScan(long scanHandle);
}
```

(`MyBridgeBackend implements ScanBackend` forwards each method to
`BridgeNative` — pure boilerplate the scaffold generates.)

Override `encodeOptions` only if the bridge already has its own options
schema (e.g. a protobuf), and `listPartitions` when the dataset should split
into more than one Spark task:

```java
    @Override
    public PartitionInfo[] listPartitions(byte[] optionsBytes) {
        MySlice[] slices = MyBridgeNative.listSlices(optionsBytes);
        PartitionInfo[] out = new PartitionInfo[slices.length];
        for (int i = 0; i < slices.length; i++) {
            out[i] = new PartitionInfo(slices[i].id(), slices[i].payload(), slices[i].hosts());
        }
        return out;
    }
```

The remaining optional methods — `sharedScan`, `reportPartitioning`, and the
filter-aware `listPartitions(opts, filters)` overload — are covered in their
own sections below. Their javadoc in
[`BridgeProviderFactory.java`](src/main/java/io/datafusion/spark/BridgeProviderFactory.java)
is the authoritative contract.

### 3. Wiring it into Spark

Either pass your factory class per read:

```python
df = (spark.read.format("datafusion")
        .option("df.factory", "com.example.MyBridgeProviderFactory")
        .option("url", "...")
        .option("table", "my_dataset")
        .load())
```

or ship a ~10-line subclass so users get a short format name:

```scala
class MyDataSource extends DatafusionSource {
  override def shortName(): String = "my_format"
  override protected def factoryFqcn(opts: CaseInsensitiveStringMap): String =
    "com.example.MyBridgeProviderFactory"
}
```

registered via a
`META-INF/services/org.apache.spark.sql.sources.DataSourceRegister` file
(this module registers `datafusion` the same way — see
[`src/main/resources/META-INF/services/`](src/main/resources/META-INF/services/)).

## Packaging your bridge

The end-user experience to aim for is one artifact:

```python
# spark.jars (or --packages) gets exactly one jar, then:
df = spark.read.format("my_format").option("url", "...").load()
```

Three pieces make that work:

**Bundle your cdylib inside the jar.** Copy it into your jar's resources at
`<your/package/path>/<os>/<arch>/<mapped name>` and load it from your native
class's static initializer with the connector's loader — no hand-rolled
extraction code:

```java
static {
    NativeLibraryLoader.load(BridgeNative.class, "com/example/mybridge", "my_bridge");
}
```

The pom side is one antrun copy execution plus per-host profiles; the
examples module is a complete working copy of the pattern (see the
`copy-example-bridge-cdylib` execution and the `native-*` profiles in
[`examples/pom.xml`](../examples/pom.xml), and the loader call in
[`ExampleBridgeNative.java`](../examples/src/main/java/org/apache/datafusion/examples/ExampleBridgeNative.java)).
For a multi-platform jar, build the cdylib per platform in CI and copy each
into its own `<os>/<arch>/` directory before `mvn package` — the layout
supports them side by side.

**Shade your dependencies into one fat jar** with `maven-shade-plugin`, so
users don't assemble a jar list:

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-shade-plugin</artifactId>
    <executions>
        <execution>
            <phase>package</phase>
            <goals><goal>shade</goal></goals>
            <configuration>
                <!-- NO <relocations>. See below. -->
                <transformers>
                    <!-- Merges DataSourceRegister service files so your short
                         name survives shading. -->
                    <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
                </transformers>
                <filters>
                    <filter>
                        <artifact>*:*</artifact>
                        <excludes>
                            <exclude>META-INF/*.SF</exclude>
                            <exclude>META-INF/*.DSA</exclude>
                            <exclude>META-INF/*.RSA</exclude>
                        </excludes>
                    </filter>
                </filters>
            </configuration>
        </execution>
    </executions>
</plugin>
```

Include in the shaded jar: this connector (`datafusion-java-spark`), the core
jar (`datafusion-java` — exception classes and, if you push predicates, the
generated proto classes), the Arrow Java artifacts you compile against, and
your own classes + cdylib. Keep `spark-sql`/`scala-library` `provided` — the
cluster supplies them.

**Do NOT relocate JNI-bound or JNI-loading packages.** JNI binds native
methods by the class's fully-qualified name; `arrow-c-data` and the Arrow
memory modules likewise load their own natives. Relocating
`io.datafusion.spark`, `org.apache.arrow`, or your own native class breaks
the symbol lookup at runtime. Practical consequences:

- Ship a plain (unrelocated) fat jar. Two bridges in one Spark app then share
  one copy of the connector classes — fine when they're built against the
  same connector version, which is the only configuration we support anyway
  (their cdylibs stay distinct via per-bridge JNI class names).
- Spark bundles its own (often older) Arrow. Since yours can't be relocated
  away, have users set `spark.executor.userClassPathFirst=true` and
  `spark.driver.userClassPathFirst=true` (the pyspark demo under
  [`examples/python/`](../examples/python/) shows the working incantation),
  or build with Arrow pinned to the cluster's version.

## Spark tasks vs. DataFusion partitions

This is the most important design decision when building a connector, so it
gets its own section.

Spark parallelism and DataFusion parallelism are different things:

- A **Spark task** is the unit Spark schedules onto an executor core. Each
  task carries fixed overhead: scheduling on the driver, (de)serializing the
  task, instantiating your factory, building a provider, planning a scan.
- A **DataFusion partition** is one output stream of a planned physical
  query. A single plan usually has several.

The connector supports two ways of mapping one onto the other:

### Default mode: one Spark task per `PartitionInfo`

`listPartitions` returns N entries → Spark runs N tasks. Each task calls
`createProvider(opts, partitionBytes)` with *its own* entry's payload, so each
task plans and scans only its slice. If DataFusion happens to plan that slice
into multiple internal partitions, they are merged into one stream for the
task — within a task there is no extra parallelism, by design (the
parallelism budget belongs to Spark).

You control the mapping entirely through what you return from
`listPartitions`. Sizing guidance:

- **Don't emit one `PartitionInfo` per tiny fragment.** A Spark task should
  do meaningfully more work than its overhead — as a rule of thumb at least
  ~100 ms of scan time, or order-100 MB of data (Spark's own file sources
  default to 128 MB per task for the same reason). If your natural unit is a
  small chunk (an object-store key, a time slice, a recording segment),
  **bin-pack several into one entry**: `partitionBytes` is opaque, so encode
  a *list* of chunk ids and have your `createProvider` materialise all of
  them in one provider.
- **Watch the total task count.** The Spark driver schedules and tracks every
  task; beyond the low thousands of tasks per stage you pay growing driver
  CPU/memory and UI lag for no extra throughput once the cluster's cores are
  saturated. A healthy target is roughly 2–3 tasks per available core, and
  rarely more than a few thousand per scan. Tens of thousands of
  single-digit-megabyte tasks is a smell — bin-pack first.
- **Locality and partition keys only exist here.** `preferredLocations`
  (host affinity) and `HasPartitionKey`/`reportPartitioning` (shuffle
  elision) are properties of `PartitionInfo` entries. If you need either,
  use this mode.

### Shared-scan mode: one Spark task per DataFusion partition

When provider construction itself is expensive (remote metadata, connection
setup) or the dataset has thousands of small natural partitions, per-task
provider builds dominate. Opting in via

```java
@Override
public boolean sharedScan(byte[] optionsBytes) { return true; }
```

flips the mapping: the provider is built **once per executor JVM per query**
(with empty `partitionBytes`), planned once, and Spark runs one task per
*DataFusion output partition* — task `i` streams plan partition `i` from the
executor-local cached plan. `listPartitions` is not called at all.

The DataFusion partition count — and therefore the Spark task count — is
pinned by `spark.datafusion.sharedScan.targetPartitions` (default 8). The
value is resolved on the driver and shipped to executors, because
DataFusion's default would vary with each machine's core count and the
partition indices must mean the same thing everywhere.

Choosing between the modes:

| Choose | When |
|--------|------|
| Default (per-partition payload) | slices have host affinity, you want partition-key semantics, per-slice provider construction is cheap. Bin-pack small slices before abandoning this mode. |
| Shared-scan | provider construction is expensive, there are thousands of small partitions with no locality story, the workload is scan + filter + projection. Provider builds drop from one-per-task to one-per-executor (plus one driver probe per query). |

Shared-scan's price of admission is a **determinism contract**: the
provider's schema, partitioning, and per-partition contents must be a pure
function of `optionsBytes`. Remote sources must pin a snapshot
(version/timestamp) inside the options. The connector fails tasks when an
executor's partition count diverges from the driver's, but equal counts with
different contents are undetectable by construction. The provider's
`ExecutionPlan` must also tolerate `execute(i)` being called more than once
per plan instance (Spark retries and speculatively re-executes tasks). Full
contract: `BridgeProviderFactory.sharedScan` javadoc.

Shared-scan operational details:

- Executor cache ([`SharedScanCache.scala`](src/main/scala/io/datafusion/spark/SharedScanCache.scala)):
  entries keyed per query (`scanId`), refcounted by open readers, evicted
  after an idle TTL. Build failures are not cached; eviction between task
  waves just rebuilds.
- Spark conf (read on the driver at planning time, shipped to executors):
  - `spark.datafusion.sharedScan.targetPartitions` (default 8)
  - `spark.datafusion.sharedScan.batchSize` (default 8192)
  - `spark.datafusion.sharedScan.idleTtlMs` (default 120000)

## What the connector does for you

- **Schema inference** — your provider's Arrow schema, widened, becomes the
  Spark schema. Driver-side, one probe build with empty `partitionBytes`.
- **Type widening** — Spark's columnar readers reject several Arrow types
  DataFusion happily produces. The SDK (inside your bridge's cdylib)
  transparently casts
  unsigned ints → wider signed, `Float16` → `Float32`, `Time*` → wider ints,
  any-unit/tz `Timestamp` → microsecond, recursively through
  `List`/`LargeList`/`FixedSizeList` (see
  [`native/src/widening.rs`](native/src/widening.rs)). Caveat: unsigned types
  nested inside `Struct`/`Map` are not yet covered.
- **Predicate pushdown** — Spark V2 `Predicate`s are translated to DataFusion
  expressions ([`SparkPredicateTranslator.scala`](src/main/scala/io/datafusion/spark/SparkPredicateTranslator.scala)),
  shipped as `datafusion-proto` bytes, and applied inside the native plan, so
  your provider's `supports_filters_pushdown`/`scan` sees real Rust `Expr`s.
  Anything untranslatable stays in Spark as a residual filter — over-claiming
  is impossible by construction.
- **Column pruning** — Spark's required-columns projection becomes a
  DataFusion projection on the native plan.
- **Partition-aware joins/aggregations** (default mode, optional) — declare
  `reportPartitioning` + per-partition key values and Spark can elide
  shuffles. See the javadoc on
  [`ReportedPartitioning.java`](src/main/java/io/datafusion/spark/ReportedPartitioning.java)
  and [`PartitionInfo.java`](src/main/java/io/datafusion/spark/PartitionInfo.java);
  note Spark 3.3+ additionally requires
  `spark.sql.sources.v2.bucketing.enabled=true` for storage-partitioned
  joins.

## What runs where

| Phase | Where | Path |
| ----- | ----- | ---- |
| Schema inference | Driver | `factory.encodeOptions` → `backend.providerSchemaIpc(opts, EMPTY)` — bridge cdylib builds + widens the provider, returns the Arrow schema |
| Scan planning (default mode) | Driver | `factory.listPartitions(opts[, filters])` → one task per entry, with its `partitionBytes` + `preferredLocations` |
| Scan planning (shared-scan) | Driver | probe build (same code path executors use) → plan partition count `N` → `N` tasks |
| Predicate translation | Driver | `SparkPredicateTranslator` → proto bytes per pushed predicate |
| Per-task scan (default mode) | Executor | `backend.createScan(opts, partitionBytes, ...)` (build provider, widen, project, filter, plan) → stream whole plan |
| Per-task scan (shared-scan) | Executor | cache-acquire by `scanId` (first task builds) → stream plan partition `i` → release |

The native machinery backing all of this is
[`bridge/src/scan.rs`](bridge/src/scan.rs), exported into each bridge's
cdylib by `export_bridge!` and reached through its [`ScanBackend`](src/main/java/io/datafusion/spark/ScanBackend.java).

## Module layout

```
spark/
├── src/main/java/io/datafusion/spark/    public SPI (Java on purpose:
│                                         bridge jars stay Scala-free)
│     BridgeProviderFactory.java            <- the contract you implement
│     ScanBackend.java                      <- native scan surface (delegations
│                                              to your bridge's JNI class)
│     NativeLibraryLoader.java              <- bundled-cdylib extraction/loading
│     PartitionInfo.java                    <- one entry = one Spark task
│     ReportedPartitioning.java             <- optional shuffle-elision declaration
├── src/main/scala/io/datafusion/spark/   connector internals (DSv2 wiring,
│                                         readers, pushdown, shared-scan cache)
└── bridge/                               datafusion-spark-bridge SDK rlib:
                                          widening + scan machinery +
                                          export_bridge! (the native side of
                                          every bridge cdylib)
```

## Caveats

- Pushed filter expressions are deserialized with DataFusion's default
  logical-extension codec, which covers columns, literals, and built-in
  functions. Anything the Spark-side translator can't express stays in Spark
  as a residual filter, so coverage gaps cost performance, never
  correctness.
- The bridge cdylib's DataFusion version is the SDK's: cargo resolves one
  `datafusion` for your provider and the scan machinery together, pinned in
  this repo's workspace [`Cargo.toml`](../Cargo.toml). Upgrading DataFusion
  means rebuilding the bridge against a newer SDK.
- The SDK's Tokio runtime is per-cdylib and `Once`-gated; TLS-using bridges
  should `Once`-gate their rustls install the same way.
