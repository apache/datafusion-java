# Apache DataFusion Java

Java bindings for [Apache DataFusion]. Queries run in native Rust and results
return to the JVM as [Apache Arrow] batches via the Arrow C Data Interface.

[Apache DataFusion]: https://datafusion.apache.org/
[Apache Arrow]: https://arrow.apache.org/

> Early development: the API will change between releases. Bug reports
> and contributions welcome.

## Install

Released to [Maven Central](https://central.sonatype.com/artifact/org.apache.datafusion/datafusion-java).
The JAR bundles the native library for Linux, macOS, and Windows on
x86_64 / aarch64.

Maven:

```xml
<dependency>
    <groupId>org.apache.datafusion</groupId>
    <artifactId>datafusion-java</artifactId>
    <version>0.1.0</version>
</dependency>
```

Gradle:

```kotlin
implementation("org.apache.datafusion:datafusion-java:0.1.0")
```

Arrow needs `--add-opens=java.base/java.nio=ALL-UNNAMED` on the JVM
command line. See the [installation guide](docs/source/user-guide/installation.md)
for details and for building from source.

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
            // ...
        }
    }
}
```

`SessionContext` and `DataFrame` are `AutoCloseable` and not thread-safe.

## Documentation

The full documentation lives under [`docs/source/`](docs/source/index.md)
and is built with Sphinx (see [`docs/README.md`](docs/README.md) for the
build steps):

- [User guide](docs/source/user-guide/index.md) — installation, the
  DataFrame and SQL APIs, Parquet ingestion.
- [Contributor guide](docs/source/contributor-guide/index.md) — build,
  test, code style, and how to bump the DataFusion version.

## Requirements

JDK 17+. Building from source: see
[`docs/source/contributor-guide/development.md`](docs/source/contributor-guide/development.md).

## Contributing

Open an issue to discuss non-trivial changes before sending a PR. See the
[contributor guide](docs/source/contributor-guide/index.md).

## License

Apache License 2.0. See [LICENSE.txt](LICENSE.txt) and [NOTICE.txt](NOTICE.txt).
