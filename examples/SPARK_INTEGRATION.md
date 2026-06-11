# Using an FFI TableProvider as a Spark Data Source

The FFI handover is simple: Rust builds an `FFI_TableProvider`, hands the raw
pointer to the JVM, and the JVM passes it to the connector cdylib
(`FfiHelperNative.createScan`), which does everything DataFusion-side in
process — widening, session construction, projection, pushed filters,
planning, and partition streams.

That flow plugs into Apache Spark as a DataSource V2 by way of the
[`connector-core`](https://github.com/rerun-io/rerun-spark-connector) module
(generic Spark plumbing donated upstream-ready). Below is the recipe for
wiring a domain bridge — e.g. an in-house format or a custom catalog — into
Spark via this pattern.

## Architecture

```
+--------------------------+                +------------------------------+
| Your bridge cdylib       |  byte[] opts   | Your bridge JVM glue         |
|  - Rust JNI:             | <----+         |  - Java POJO + proto encoder |
|    createFfiProvider     |      |         |  - FfiProviderFactory impl   |
|    listPartitions        |      |  jlong  |  - System.load(cdylib)       |
|  - FFI_TableProvider     | <----+----+----+-------- driver / executor    |
+--------------------------+      raw ptr   +------------------------------+
                                                       |
                                                       v
+--------------------------+                +------------------------------+
| connector-core cdylib    |   jlong ptr    | connector-core JVM           |
|  - WideningTableProvider | <------------- |  - DatafusionSource (DSv2)   |
|    over arrow::cast      |                |  - SparkPredicateTranslator  |
|  - createScan: session,  |  FFI_Arrow-    |  - ColumnarPartitionReader   |
|    projection, filters,  |  ArrayStream   |  - SharedScanCache           |
|    plan, exec partitions | -------------> |                              |
+--------------------------+                +------------------------------+
```

Key invariants:

- Only the opaque `FFI_TableProvider` pointer crosses the cdylib boundary
  (and `FFI_ArrowArrayStream` on the way back). No `SessionContext` is ever
  shared, and none exists JVM-side — planning and execution live entirely in
  the connector cdylib.
- The connector cdylib widens between your bridge's provider and the scan:
  it casts Spark-incompatible Arrow types (UInt*, Float16,
  Time*, non-µs Timestamp, recursive List/LargeList/FixedSizeList) using
  kernel-level `arrow::compute::cast`. No SQL, no view rewrites.
- Predicate pushdown crosses the FFI boundary as a `LogicalExprNode` proto
  (datafusion-proto). Spark translates V2 `Predicate`s and ships the bytes;
  the producer's `TableProviderFilterPushDown::scan(...)` sees them as Rust
  `Expr`s.

## Producer side (Rust)

Your bridge cdylib exposes a `createFfiProvider` JNI entrypoint that decodes
your domain proto, builds an `Arc<dyn TableProvider>`, and wraps it in
`FFI_TableProvider`. This is exactly what
[`examples/native/src/lib.rs`](native/src/lib.rs) does for a `MemTable`. For
a real bridge, replace the `MemTable` with your own `TableProvider`
implementation:

```rust
let provider: Arc<dyn TableProvider> = runtime().block_on(build_provider(opts))?;
let ffi = FFI_TableProvider::new(
    provider,
    /*can_support_pushdown_filters=*/ true,
    Some(runtime().clone()),
    FFI_TaskContextProvider::from(&ctx_provider),  // throwaway local SessionContext
    /*logical_codec=*/ None,                       // default DataFusion codec
);
Box::into_raw(Box::new(ffi)) as jlong
```

Driver-side partition enumeration goes through a second JNI entrypoint
`listPartitions(options_proto_bytes) -> PartitionInfo[]`. One Spark task gets
created per returned entry. Each `PartitionInfo` carries:

- `id` — stable, human-readable partition identifier (surfaces in Spark UI/logs).
- `partitionBytes` — opaque per-partition payload, replayed into
  `createProvider(opts, partitionBytes)` so the executor materialises *this*
  slice. Empty array = no per-partition state.
- `preferredLocations` — hostnames where this slice's data lives. Spark uses
  these (subject to `spark.locality.wait`) to co-locate the task with the
  data — e.g. four partitions per worker on a 3-worker cluster.

## JVM glue

Implement `io.datafusion.spark.FfiProviderFactory` (from
[`connector-core`](https://github.com/rerun-io/rerun-spark-connector/blob/main/connector-core/src/main/java/io/datafusion/spark/FfiProviderFactory.java)).
Must be no-arg constructable so executors can instantiate it via
`Class.forName(...).getDeclaredConstructor().newInstance()`.

```java
public final class MyBridgeProviderFactory implements FfiProviderFactory {

    @Override
    public byte[] encodeOptions(Map<String, String> sparkOptions) {
        // Translate Spark options ("url", "table", ...) into your proto.
        return MyBridgeOptions.fromMap(sparkOptions).toProtoBytes();
    }

    @Override
    public PartitionInfo[] listPartitions(byte[] optionsProtoBytes) {
        // Bridge enumerates slices and resolves their host placement:
        //   record MySlice(String id, byte[] payload, String[] hosts) {}
        MySlice[] slices = MyBridgeNative.listSlices(optionsProtoBytes);
        PartitionInfo[] out = new PartitionInfo[slices.length];
        for (int i = 0; i < slices.length; i++) {
            out[i] = new PartitionInfo(slices[i].id(), slices[i].payload(), slices[i].hosts());
        }
        return out;
    }

    @Override
    public long createProvider(byte[] optionsProtoBytes, byte[] partitionBytes) {
        // partitionBytes is the same payload returned from listPartitions for *this* task.
        // The driver-side schema probe passes an empty array; honour it.
        return MyBridgeNative.createFfiProvider(optionsProtoBytes, partitionBytes);
    }

    @Override
    public ReportedPartitioning reportPartitioning(byte[] optionsProtoBytes) {
        // Optional. Return non-null only when each PartitionInfo's rows all share the same
        // key tuple under the declared transforms — Spark elides shuffles ahead of joins/aggs
        // grouped on those keys. Return null when the layout is unknown or row-key mapping
        // would be lossy.
        return ReportedPartitioning.identity("device_id");
        // or: ReportedPartitioning.bucket(numBuckets, "user_id");
    }
}
```

## Wiring it into Spark

Two paths, pick one:

### Option A — config option per use

```python
df = (spark.read.format("datafusion")
        .option("df.factory", "com.example.MyBridgeProviderFactory")
        .option("url", "rerun+http://localhost:51234")
        .option("table", "my_dataset")
        .load())
df.printSchema()
df.filter("ts > 1700000000").show()
```

### Option B — thin shim with a short name

Mirror the
[`rerun-connector`](https://github.com/rerun-io/rerun-spark-connector/blob/main/rerun-connector/src/main/scala/io/rerun/spark/RerunDataSource.scala)
shim — a ~20-line subclass that bakes the factory FQCN in:

```scala
class MyDataSource extends DatafusionSource {
  override def shortName(): String = "my_format"
  override protected def factoryFqcn(opts: CaseInsensitiveStringMap): String =
    "com.example.MyBridgeProviderFactory"
}
```

Register via `META-INF/services/org.apache.spark.sql.sources.DataSourceRegister`,
then:

```python
df = (spark.read.format("my_format")
        .option("url", "...")
        .option("table", "...")
        .load())
```

## What runs where

| Phase                       | Where     | Path |
| --------------------------- | --------- | ---- |
| `inferSchema`               | Driver    | `factory.encodeOptions` → `factory.createProvider(opts, EMPTY)` → `FfiHelperNative.providerSchemaIpc` (widens, returns Arrow IPC schema) |
| `ScanBuilder.build`         | Driver    | `factory.listPartitions(optionsBytes, filterBytes)` (filter-aware overload — bridges can prune partitions; cached on Scan) + `factory.reportPartitioning(optionsBytes)` (cached on Scan) |
| `outputPartitioning`        | Driver    | `KeyGroupedPartitioning(reported.keys, partitions.length)` when bridge declared one; `UnknownPartitioning(partitions.length)` otherwise. Spark may elide shuffles when keys line up with downstream join/agg grouping. |
| `planInputPartitions`       | Driver    | Reuses the cached `PartitionInfo[]`; one task per entry with that entry's `partitionBytes` + `preferredLocations` |
| Predicate translation       | Driver    | `SparkPredicateTranslator.translate(Predicate)` → `LogicalExprNode` proto bytes (each pushed predicate is independent) |
| Per-task scan               | Executor  | Same factory → `createProvider(opts, partitionBytes)` → `FfiHelperNative.createScan` (widen, projection, pushed proto filters, plan) → `executeStream` |

## Partition key values (`HasPartitionKey`)

Declaring `reportPartitioning` alone is NOT enough on Spark 3.3+: Spark's
`DataSourceV2ScanExecBase.groupPartitions` only consumes the declared
`KeyGroupedPartitioning` when every input partition also implements
`HasPartitionKey`. To activate it, return the key values per partition via
`PartitionInfo`'s 4-argument constructor:

```java
new PartitionInfo(slice.id(), slice.payload(), slice.hosts(),
                  new Object[] {slice.segmentId()});  // matches identity("segment_id")
```

Rules: all partitions carry keys or none (mixed state fails the scan
driver-side); array arity must equal the declared key count; values must be
`CatalystTypeConverters`-convertible Java types (`String`, `Long`,
`java.time.Instant`, `java.time.LocalDate`, `java.math.BigDecimal`, ...).
Storage-partitioned joins additionally require
`spark.sql.sources.v2.bucketing.enabled=true`.

## Shared-scan mode

The default model above builds one provider per Spark task. For datasets with
thousands of small partitions — or providers whose construction is expensive
(remote metadata, connection setup) — the per-task fixed cost dominates.
Shared-scan mode flips the mapping: the bridge's provider is built ONCE per
(executor JVM × query) with empty `partitionBytes`, planned once, and Spark
runs one task per *DataFusion-native* output partition; task `i` streams plan
partition `i` from the cached plan.

Opt in per dataset from the factory:

```java
@Override
public boolean sharedScan(byte[] optionsProtoBytes) {
    return MyBridgeOptions.fromProtoBytes(optionsProtoBytes).useSharedScan();
}
```

What changes:

| Phase                  | Where    | Path |
| ---------------------- | -------- | ---- |
| `ScanBuilder.build`    | Driver   | mint `scanId` (UUID) + pin session config → probe build (same code path as executors) → physical plan partition count `N` → `N` tasks |
| `outputPartitioning`   | Driver   | always `UnknownPartitioning(N)` — DataFusion partitions carry no key contract; `listPartitions` / `reportPartitioning` are not called |
| Per-task scan          | Executor | `SharedScanCache.acquire(scanId)` → (first task only) `createProvider(opts, EMPTY)` → `FfiHelperNative.createScan` with the pinned config (widen, projection, filters, plan once) → every task `executeStreamPartition(partitionIndex)` → release |

Cache semantics: entries are keyed by `scanId` (per query — separate actions
build separate entries), refcounted by open readers, and evicted after an idle
TTL. Build failures are not cached; eviction between task waves just rebuilds.

Spark conf (all read driver-side at planning time and shipped to executors):

- `spark.datafusion.sharedScan.targetPartitions` (default 8) — pinned
  DataFusion `target_partitions`. Any constant works; it must merely be the
  same everywhere, which shipping guarantees.
- `spark.datafusion.sharedScan.batchSize` (default 8192)
- `spark.datafusion.sharedScan.idleTtlMs` (default 120000) — cache idle
  eviction window.

**Determinism contract** (the price of admission — see
`FfiProviderFactory.sharedScan` Javadoc): the provider's schema, partitioning,
and per-partition contents must be a pure function of `optionsProtoBytes`.
Remote sources must pin a snapshot (version/timestamp) inside the options.
The connector fails tasks when an executor's partition count diverges from the
driver's, but equal counts with different contents are undetectable. The
provider's `ExecutionPlan` must also tolerate `execute(i)` being called more
than once per plan instance (task retry / speculative execution).

Choosing a model:

- **Per-partition payload (default)** — slices have host affinity
  (`preferredLocations`), per-slice provider construction is cheap, or you
  want `KeyGroupedPartitioning` + `HasPartitionKey` semantics. Bin-pack many
  small slices into fewer `PartitionInfo` entries via `partitionBytes` (it is
  opaque — encode a list of slice ids) before reaching for shared-scan.
- **Shared-scan** — thousands of small partitions, expensive
  `createProvider`, no locality story, scan+filter+projection workloads.
  Provider builds drop from one-per-task to one-per-executor (plus one driver
  probe per query).

## Caveats

- One `FFI_LogicalExtensionCodec` per provider — v1 uses
  `DefaultLogicalExtensionCodec`. If your bridge serializes custom
  `LogicalNode`s, swap the codec at `FFI_TableProvider::new` time.
- Each cdylib brings its own Tokio runtime and (for TLS-using bridges) its
  own rustls install. Both should be `Once`-gated.
- The widening cdylib in `connector-core` covers top-level scalars + List
  children. Nested unsigned inside `Struct`/`Map` still surfaces the raw
  Arrow type to Spark and fails at column-vector accessor time. Extend
  `arrow_cast_widening` if you hit this.
