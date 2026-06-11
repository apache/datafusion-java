# DataFusion-Java examples

Self-contained Java programs against the DataFusion-Java API.

`exec:exec` (not `exec:java`) runs each one — the pom shells out to a fresh
`java` process so the JNI library's `--add-opens=java.base/java.nio=ALL-UNNAMED`
JVM flag actually applies.

`exec:exec` is a separate Maven invocation from the one that built the
project, so it resolves `datafusion-java` from your local Maven repository
rather than the reactor's `target/` dirs. That means the parent must be
**installed** to the local repo first — `package -am` builds the jar but
does NOT publish it, which surfaces as
`Could not find artifact org.apache.datafusion:datafusion-java:jar:0.2.0-SNAPSHOT`.

```bash
# Install the fork into your local Maven repo, then run any example.
mvn -B install -DskipTests -Drat.skip=true \
    -Ddatafusion.native.profile=release
mvn -B -pl examples exec:exec \
    -Dexec.mainClass=org.apache.datafusion.examples.<ClassName>
```

(If your local Maven repo lives somewhere other than `~/.m2/repository`,
add `-Dmaven.repo.local=/path/to/repo` to BOTH invocations.)

| Class                            | What it shows                                                                                 |
| -------------------------------- | --------------------------------------------------------------------------------------------- |
| `SqlQueryExample`                | Register a CSV file and run a SQL aggregation.                                                |
| `DataFrameExample`               | DataFrame API: filter, group, sort.                                                           |
| `ProtoPlanExample`               | Build a `LogicalPlanNode` proto in Java, hand it to `SessionContext.fromProto`.               |
| `JdbcExample`                    | Pull from an H2 JDBC source into Arrow, register it, query.                                   |
| `AddOneExample`                  | Implement a Scalar UDF in Java and register it on the session.                                |
| `NestedTypeUdfExample`           | Scalar UDF over `List<Int64>` — input + output nested arrow types.                            |
| `ExampleFfiProviderFactory`      | Build an `FFI_TableProvider` in Rust (a `MemTable`) and expose it to Spark through the connector's `FfiProviderFactory` interface. **See: [SPARK_INTEGRATION.md](SPARK_INTEGRATION.md) and the pyspark demo under [`python/`](python/).** |

## Building the FFI example's cdylib

The FFI provider examples rely on a small Rust cdylib under
[`native/`](native/). It is a member of the repo-root Cargo workspace, so
build it by name from anywhere in the tree:

```bash
cargo build -p datafusion-java-ffi-example --release
```

The example's `System.load` searches the following paths in order:

1. `-Dexample.ffi.lib.path=/abs/path/to/lib...` (explicit override)
2. `rust-target/release/<mappedName>` (Maven's cwd is the repo root)
3. `rust-target/debug/<mappedName>`
4. `../rust-target/release/<mappedName>` (cwd inside the `examples` module)
5. `../rust-target/debug/<mappedName>`

Where `<mappedName>` is `libdatafusion_java_ffi_example.so` on Linux,
`libdatafusion_java_ffi_example.dylib` on macOS, or
`datafusion_java_ffi_example.dll` on Windows.
