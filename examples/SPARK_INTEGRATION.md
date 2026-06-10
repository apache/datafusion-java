# Using an FFI TableProvider as a Spark Data Source

The [`FfiTableProviderExample`](src/main/java/org/apache/datafusion/examples/FfiTableProviderExample.java)
shows the JVM side of the FFI handover: Rust builds an `FFI_TableProvider`,
hands the raw pointer to the JVM, and the JVM calls
`SessionContext.registerFfiTable(name, ptr)` to make it queryable through
DataFusion-Java.

That same flow plugs into Apache Spark as a DataSource V2 by way of the
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
| connector-core cdylib    |  jlong (wide)  | connector-core JVM           |
|  - WideningTableProvider | <------------- |  - DatafusionSource (DSv2)   |
|    over arrow::cast      |                |  - SparkPredicateTranslator  |
+--------------------------+                |  - ColumnarPartitionReader   |
                                            +------------------------------+
                                                       |
                                                       v
                                            +------------------------------+
                                            | datafusion-java              |
                                            |  - SessionContext            |
                                            |  - registerFfiTable(name,ptr)|
                                            |  - DataFrame.filterFromProto |
                                            +------------------------------+
```

Key invariants:

- Only the opaque `FFI_TableProvider` pointer crosses the cdylib boundary.
  No `SessionContext` is ever shared.
- The widening cdylib (connector-core) sits between your bridge and
  `registerFfiTable`. It casts Spark-incompatible Arrow types (UInt*, Float16,
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
| `inferSchema`               | Driver    | `factory.encodeOptions` → `factory.createProvider(opts, EMPTY)` → widen → `registerFfiTable` → `ctx.tableSchema` |
| `planInputPartitions`       | Driver    | `factory.listPartitions(optionsBytes)` → one task per `PartitionInfo`; each task gets that entry's `partitionBytes` + `preferredLocations` |
| Predicate translation       | Driver    | `SparkPredicateTranslator.translate(Predicate)` → `LogicalExprNode` proto bytes (each pushed predicate is independent) |
| Per-task scan               | Executor  | Same factory → `createProvider(opts, partitionBytes)` → widen → `registerFfiTable` → `ctx.sql("SELECT proj FROM t")` → fold `DataFrame.filterFromProto(bytes)` over pushed predicates → `executeStream` |

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
