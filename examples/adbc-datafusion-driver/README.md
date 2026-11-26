<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Example: an ADBC driver for a custom DataFusion provider

End-to-end demonstration of the ADBC route ("Part A"): expose a custom
DataFusion `TableProvider` as a standard ADBC driver, then read it from the
`adbc-datafusion` Spark connector in this repo.

This crate stands in for a **separate driver repo**. It is its own Cargo
workspace (note the empty `[workspace]` in `Cargo.toml`) and is *not* part of
the parent `datafusion-java` workspace.

## What it does

- `src/provider.rs` -- `ExampleTableProvider`, an ordinary `TableProvider`
  exposing one fixed table `example(id BIGINT, name STRING)`. Replace this with
  your real provider.
- `src/lib.rs` -- `ExampleDriver`, a thin wrapper over
  [`adbc-driver-datafusion`](https://github.com/adbc-drivers/datafusion)'s
  `DataFusionDriver`. Its `Default` builds the inner driver with a
  `new_with_context_init` hook that registers the provider into every session.
  `adbc_ffi::export_driver!` emits the C `AdbcDatafusionExampleInit` entrypoint.

No fork of the driver, no `datafusion-ffi` module loading -- the provider is
compiled in.

## Build

```bash
cargo build --release
# produces target/release/libadbc_datafusion_example_driver.{so,dylib,dll}
```

## Use from the Spark connector

The `adbc-datafusion` Spark DataSource in this repo loads the cdylib through the
arrow-adbc Java driver manager. Point the `driver` option at the built library
and the `table` option at the provider's table:

```scala
val df = spark.read
  .format("adbc-datafusion")
  .option("driver", "/abs/path/to/libadbc_datafusion_example_driver.so")
  .option("table", "example")
  .load()

df.show()
// +---+-----+
// | id| name|
// +---+-----+
// |  1|alice|
// |  2|  bob|
// |  3|carol|
// +---+-----+
```

`option("target_partitions", N)` tunes scan parallelism: the connector issues
`SET datafusion.execution.target_partitions = N` on the planning session, and the
driver pins it into each partition descriptor so executors re-plan identically.
It defaults to the cluster parallelism (`SparkContext.defaultParallelism`).
Repartition-aware providers (e.g. file scans) use it to choose the partition
count; fixed-partition providers (like this in-memory example) keep their
intrinsic count.

Any other `option(...)` keys (besides `driver`, `table`, `target_partitions`) are
forwarded verbatim as ADBC database options, so a real provider can be configured
through them.

## Use from any ADBC client

The same cdylib works with any ADBC driver manager, e.g. Python:

```python
import adbc_driver_manager.dbapi as dbapi

with dbapi.connect(
    driver="/abs/path/to/libadbc_datafusion_example_driver.so",
    entrypoint="AdbcDatafusionExampleInit",
) as conn, conn.cursor() as cur:
    cur.execute("SELECT * FROM example")
    print(cur.fetch_arrow_table())
```

## Notes

- `datafusion` / `arrow` versions are pinned to match `adbc-driver-datafusion`
  (datafusion 53, arrow 58) so the shared provider type is ABI-compatible across
  the driver and any in-repo Rust that registers the same provider.
- Pushdown: ADBC clients send SQL or a Substrait plan; DataFusion's optimizer
  applies projection/filter/limit against the provider. The Spark connector
  pushes a Substrait plan with projection/filter/limit folded in.
- Multi-partition: with the `execute_partitions` / `read_partition` driver
  support (adbc-drivers/datafusion#32) and arrow-adbc >= 0.24's JNI bridge, the
  Spark connector reads one partition per provider partition; against older
  arrow-adbc it degrades to a single partition. See
  [PARTITIONS.md](PARTITIONS.md) for the driver-side code.
