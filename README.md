# Apache DataFusion Java

Java bindings for [Apache DataFusion], the Rust-based query engine. SQL queries
run in native code and results are returned to the JVM as [Apache Arrow] record
batches via the Arrow C Data Interface — no per-row JNI calls, no row-by-row
copies.

[Apache DataFusion]: https://datafusion.apache.org/
[Apache Arrow]: https://arrow.apache.org/

> **Project status: early development.** This is a brand-new project. The API
> is small, will change without notice, and there is no published release. Do
> not depend on it from production code yet. Bug reports, design feedback, and
> contributions are very welcome.

## Quickstart

```java
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.datafusion.DataFrame;
import org.apache.datafusion.SessionContext;

try (var allocator = new RootAllocator();
     var ctx = new SessionContext()) {

    ctx.registerParquet("orders", "/path/to/orders.parquet");

    try (DataFrame df = ctx.sql(
            "SELECT o_orderpriority, COUNT(*) AS n " +
            "FROM orders GROUP BY o_orderpriority");
         ArrowReader reader = df.collect(allocator)) {
        while (reader.loadNextBatch()) {
            var batch = reader.getVectorSchemaRoot();
            // ... consume batch ...
        }
    }
}
```

The current public surface mirrors a small slice of the Rust DataFusion API:

- `SessionContext.sql(String query)` — parse and plan a SQL query, returning a
  lazy `DataFrame`. No execution happens yet.
- `DataFrame.collect(BufferAllocator allocator)` — execute the plan and return
  the result batches as an `ArrowReader`. Consumes the DataFrame; the caller
  closes the reader, and the allocator must outlive it.
- `SessionContext.registerParquet(String name, String path)` — register a local
  Parquet file as a SQL table.

Both `SessionContext` and `DataFrame` are `AutoCloseable` and **not
thread-safe**.

## Prerequisites

- JDK 17 or newer
- Rust toolchain (stable, installed via [rustup])
- [`tpchgen-cli`] — only needed to generate test data for the Parquet
  integration test (`cargo install tpchgen-cli`)

Maven is bundled via the `./mvnw` wrapper; no separate Maven install required.

[rustup]: https://rustup.rs/
[`tpchgen-cli`]: https://github.com/clflushopt/tpchgen-rs

## Build & test

    make test

This builds the native Rust crate and runs the JUnit tests. The steps can be
run individually:

    cd native && cargo build
    ./mvnw test

The native library must be built before running JVM tests.

## Test data

The Parquet integration test reads TPC-H SF1 data (~345 MB across 8 tables in
Snappy-compressed Parquet). Generate it once with:

    make tpch-data

Tests that need this data skip cleanly if it is missing. `make clean` does
**not** remove `tpch-data/` — delete it manually to reclaim the disk space.

## Repository layout

- `src/` — Java sources and tests
- `native/` — Rust crate that exposes DataFusion over JNI and the Arrow C Data
  Interface

## Roadmap

Near-term priorities, roughly in order:

- **Session configuration.** Expose `SessionConfig` and `RuntimeEnv` settings
  (target partitions, batch size, memory pool, default catalog, …) so callers
  can tune execution from the JVM.
- **Full `SessionContext` and `DataFrame` APIs.** Expand beyond `sql` and
  `registerParquet` to mirror the Rust API: table registration variants,
  `read_*` / `write_*` entry points, and DataFrame transformations such as
  `select`, `filter`, `join`, `aggregate`, `sort`, plus result-materialization
  variants (`show`, `count`, streaming collection).
- **JVM-side plan construction via Protobuf.** Build DataFusion logical and
  physical plans on the JVM using the existing DataFusion Protobuf
  representation, then ship them to the native side for execution. This lets
  plans assembled by other JVM tools (Spark, Flink, Beam, custom planners) run
  on DataFusion without having to go through SQL.
- **Java-defined expressions.** Allow user expressions / UDFs to be implemented
  in Java and invoked from the native plan, operating on Apache Arrow vectors
  (Java side) so evaluation stays vectorized end-to-end and avoids row-by-row
  JNI crossings.

These are intentionally large items — design discussion via GitHub issues
before implementation is welcome.

## Contributing

This project follows the Apache DataFusion contribution model. Issues and pull
requests are welcome — please open a GitHub issue to discuss any significant
change before sending a PR.

## License

Licensed under the [Apache License, Version 2.0](LICENSE.txt). See
[NOTICE.txt](NOTICE.txt) for required attributions.
