# DataFusion-Java examples

Small, self-contained programs that each demonstrate one feature of the
DataFusion-Java API. Every example is a Java class with a `main` method that
builds a query against an in-process DataFusion engine and prints its result
(as tab-separated rows) to stdout. They are the fastest way to see what the
library can do and to copy a working starting point.

## Prerequisites

- JDK 17+
- Maven (the repo ships `./mvnw`, no install needed)
- Rust toolchain (`cargo`) — the library calls into a native DataFusion
  build, so the Rust side must be compiled once first

## Build once

From the repo root:

```bash
# 1. Compile the native libraries (DataFusion + JNI glue).
cargo build --release

# 2. Build the Java/Scala modules and install them into your local Maven repo.
./mvnw -B install -DskipTests -Drat.skip=true -Ddatafusion.native.profile=release
```

Step 2 must be `install`, not `package`: running an example below starts a
fresh Maven invocation that resolves `datafusion-java` from your local Maven
repository (`~/.m2/repository`), and only `install` publishes the jar there.
If you skip it you'll see
`Could not find artifact org.apache.datafusion:datafusion-java:...` —
that error means "run step 2".

(If your local Maven repo lives somewhere non-standard, add
`-Dmaven.repo.local=/path/to/repo` to step 2 **and** to every run command.)

## Run your first example

```bash
./mvnw -B -pl examples exec:exec \
    -Dexec.mainClass=org.apache.datafusion.examples.SqlQueryExample
```

This registers a small CSV file, runs a SQL aggregation over it, and prints
the result rows. Swap `SqlQueryExample` for any class in the table below.

> Why `exec:exec` and not `exec:java`? Each example runs in a fresh `java`
> process so the JVM flag the native Arrow integration needs
> (`--add-opens=java.base/java.nio=ALL-UNNAMED`) actually applies. `exec:java`
> would run inside Maven's own JVM without it.

## The examples

| Entry point (`-Dexec.mainClass=org.apache.datafusion.examples.<…>`) | Demonstrates | What you'll see |
| --- | --- | --- |
| `SqlQueryExample` | Register a CSV file, run a SQL aggregation | The aggregated rows printed as TSV |
| `DataFrameExample` | The DataFrame API: filter, group, sort — no SQL strings | The transformed rows |
| `ProtoPlanExample` | Build a DataFusion `LogicalPlanNode` protobuf in Java and execute it via `SessionContext.fromProto` — the wire-format path used by query frontends | The plan's result rows |
| `JdbcExample` | Pull rows from a JDBC source (in-memory H2) into Arrow, register them as a table, query them | Rows that originated in H2, queried through DataFusion |
| `AddOneExample` | Write a scalar UDF in Java and call it from SQL | Each input value, plus one |
| `NestedTypeUdfExample` | A scalar UDF whose input and output are nested Arrow types (`List<Int64>`) | The transformed list column |

## The Spark connector example

One example is not a standalone `main`:
`ExampleFfiProviderFactory` implements the Spark connector's
`FfiProviderFactory` interface over a tiny Rust-built in-memory table (the
cdylib under [`native/`](native/)). It exists to be loaded *by Spark* — the
runnable end-to-end version is the PySpark demo under
[`python/`](python/), and the guide to building your own connector is
[`../spark/README.md`](../spark/README.md).

To build its cdylib (workspace member, buildable from anywhere in the tree):

```bash
cargo build -p datafusion-java-ffi-example --release
```

The factory's `System.load` searches, in order:

1. `-Dexample.ffi.lib.path=/abs/path/to/lib...` (explicit override)
2. `rust-target/release/<mappedName>` (cwd = repo root)
3. `rust-target/debug/<mappedName>`
4. `../rust-target/release/<mappedName>` (cwd = the `examples` module)
5. `../rust-target/debug/<mappedName>`

where `<mappedName>` is `libdatafusion_java_ffi_example.so` (Linux),
`libdatafusion_java_ffi_example.dylib` (macOS), or
`datafusion_java_ffi_example.dll` (Windows).

## Troubleshooting

- **`Could not find artifact org.apache.datafusion:datafusion-java`** — the
  parent wasn't installed to your local Maven repo. Re-run build step 2
  (`install`, not `package`).
- **`Native library not found ...`** — the Rust side wasn't built, or was
  built in a different profile than Maven expects. Re-run build step 1 and
  keep `-Ddatafusion.native.profile=release` consistent between the cargo
  profile (`--release`) and the Maven flag.
- **`UnsatisfiedLinkError ... datafusion_java_ffi_example`** — only the FFI
  example's cdylib is missing; see "The Spark connector example" above.
