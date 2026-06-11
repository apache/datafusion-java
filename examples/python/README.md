# PySpark end-to-end demo

`bridge_demo.py` proves the full DataFusion → Spark path:

```
examples/native (export_bridge! cdylib)   <-- in-memory MemTable + scan machinery
   ^  byte[] options / FFI_ArrowArrayStream
   |
ExampleBridgeProviderFactory              <-- implements BridgeProviderFactory
   |  Class.forName(...)
   v
datafusion-java-spark                     <-- DSv2 plumbing, predicate xlate
   |  spark.read.format("datafusion")
   v
PySpark DataFrame                         <-- printSchema / show / filter / select
```

## Prerequisites

1. **Java 17.** `JAVA_HOME` must point at a JDK 17 install.

2. **The example bridge cdylib** built from this repo:

   ```bash
   cargo build -p datafusion-java-example-bridge --release
   ```

3. **Maven artifacts installed into a side-loaded local repository.**

   The script reads `arrow-c-data`, `flatbuffers-java`, and `protobuf-java`
   jars from `${DATAFUSION_DEMO_M2:-/tmp/m2-datafusion}` (Spark's bundled
   versions are too old, so the demo prepends our copies on
   `spark.driver/executor.extraClassPath`). Tell Maven to install there:

   ```bash
   mvn install -DskipTests \
       -Ddatafusion.native.profile=release \
       -Dmaven.repo.local=/tmp/m2-datafusion
   ```

   If you already use `~/.m2`, point `DATAFUSION_DEMO_M2` at it instead and
   skip `-Dmaven.repo.local`.

4. **A Scala 2.13 Spark distribution.** The PyPI `pyspark` wheel embeds
   Scala 2.12 jars; the connector is compiled against 2.13, so we override
   `SPARK_HOME` before importing pyspark. Download once:

   ```bash
   cd /tmp
   curl -L -o spark-2.13.tgz \
     https://archive.apache.org/dist/spark/spark-3.5.7/spark-3.5.7-bin-hadoop3-scala2.13.tgz
   tar xzf spark-2.13.tgz
   ```

   The script defaults `SPARK_HOME` to
   `/tmp/spark-3.5.7-bin-hadoop3-scala2.13`; set the env var if you put it
   elsewhere.

5. **A self-contained Python venv with `pyspark==3.5.7`** (uv keeps it
   isolated from system site-packages):

   ```bash
   cd examples/python
   uv venv --python 3.11 .venv
   uv pip install --python .venv/bin/python "pyspark==3.5.7"
   cd ../..
   ```

## Run

```bash
examples/python/.venv/bin/python examples/python/bridge_demo.py
```

Expected output:

```
=== schema ===
root
 |-- id: long (nullable = false)
 |-- name: string (nullable = true)
 |-- value: double (nullable = true)

=== full scan ===
+---+-----+-----+
|id |name |value|
+---+-----+-----+
|1  |alice|1.5  |
|2  |bob  |2.5  |
|3  |NULL |3.5  |
|4  |dave |NULL |
+---+-----+-----+

=== filter pushdown: value > 2.0 ===
+---+----+-----+
|id |name|value|
+---+----+-----+
|2  |bob |2.5  |
|3  |NULL|3.5  |
+---+----+-----+

=== projection: id, name ===
+---+-----+
|id |name |
+---+-----+
|1  |alice|
|2  |bob  |
|3  |NULL |
|4  |dave |
+---+-----+
```

Filter row count drops from 4 → 2 because the predicate is pushed into the
bridge cdylib as a `LogicalExprNode` proto and applied inside DataFusion
before Arrow batches cross back to Spark.

## Notes

- `master("local[2]")` keeps driver + executor in one JVM so the example
  cdylib loads once. In cluster mode nothing extra is needed: the bridge
  cdylib travels inside the examples jar and `NativeLibraryLoader` extracts
  it on every worker.
- `extraClassPath` (not `--packages` / `userClassPathFirst`) is used because
  the Spark distro ships Arrow 12, flatbuffers 1.12, and protobuf 2.5, all
  of which we need to override; userClassPathFirst splits Netty across two
  class loaders and the `arrow-memory-netty-buffer-patch` shim breaks.
- The `datafusion` format short name resolves via the SPI file in
  `spark/src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister`.
  You can also use the FQCN: `format("io.datafusion.spark.DatafusionSource")`.
- To swap in your own bridge, write a `BridgeProviderFactory` against your own
  cdylib (mirroring `ExampleBridgeProviderFactory`) and pass its FQCN via
  `option("df.factory", ...)`.
