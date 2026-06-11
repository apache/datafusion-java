#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""End-to-end PySpark demo of the DataFusion FFI table provider.

Wires the in-memory example MemTable produced by ``examples/native`` into a
Spark DataSource V2 scan through the generic connector in ``spark/``.

Prerequisites (run from the repo root):

  cd native && cargo build --release && cd ..
  cd examples/native && cargo build --release && cd ../..
  cd spark/native && cargo build --release && cd ../..
  mvn install -Ddatafusion.native.profile=release -DskipTests

Run:

  python3 examples/python/ffi_table_provider_demo.py
"""

import glob
import os
import sys
from pathlib import Path

# The PyPI ``pyspark`` wheel embeds a Scala 2.12 Spark distribution; this
# connector is compiled against Scala 2.13. Override SPARK_HOME (before the
# pyspark import so the wheel honours it) to a side-loaded 2.13 distribution.
_SPARK_HOME_2_13 = os.environ.get(
    "SPARK_HOME",
    "/tmp/spark-3.5.7-bin-hadoop3-scala2.13",
)
if not Path(_SPARK_HOME_2_13, "jars", "scala-library-2.13.8.jar").exists():
    sys.exit(
        f"missing Scala 2.13 Spark distribution at {_SPARK_HOME_2_13}. "
        "Download from https://archive.apache.org/dist/spark/spark-3.5.7/"
        "spark-3.5.7-bin-hadoop3-scala2.13.tgz and extract to that path "
        "(or set SPARK_HOME to your own 2.13 distro)."
    )
os.environ["SPARK_HOME"] = _SPARK_HOME_2_13

from pyspark.sql import SparkSession


REPO_ROOT = Path(__file__).resolve().parents[2]
VERSION = "0.2.0-SNAPSHOT"
ARROW_VERSION = "19.0.0"
FLATBUFFERS_VERSION = "25.2.10"
PROTOBUF_VERSION = "3.25.5"
# Local maven repository populated by ``mvn install -Dmaven.repo.local=...``.
M2_REPO = Path(os.environ.get("DATAFUSION_DEMO_M2", "/tmp/m2-datafusion"))


def _resolve_jar(module: str, artifact: str) -> str:
    candidates = glob.glob(str(REPO_ROOT / module / "target" / f"{artifact}-{VERSION}.jar"))
    if not candidates:
        sys.exit(
            f"missing jar for {artifact} under {module}/target/. "
            f"Run 'mvn install -DskipTests' from {REPO_ROOT} first."
        )
    return candidates[0]


def _m2_jar(group_path: str, artifact: str, version: str) -> str:
    path = M2_REPO / group_path / artifact / version / f"{artifact}-{version}.jar"
    if not path.exists():
        sys.exit(
            f"missing dependency jar {path}. "
            f"Re-run 'mvn install -DskipTests -Dmaven.repo.local={M2_REPO}'."
        )
    return str(path)


def main() -> None:
    # Spark 3.5.7 bundles Arrow 12.0.1; datafusion-java is compiled against
    # Arrow 19, which needs ArrowArrayStream (added after 12) and a much newer
    # flatbuffers runtime. Ship our copies on spark.jars and force userClassPathFirst
    # so they win over the bundled jars on both driver and executor.
    arrow_jars = [
        _m2_jar("org/apache/arrow", "arrow-format", ARROW_VERSION),
        _m2_jar("org/apache/arrow", "arrow-vector", ARROW_VERSION),
        _m2_jar("org/apache/arrow", "arrow-memory-core", ARROW_VERSION),
        _m2_jar("org/apache/arrow", "arrow-memory-netty", ARROW_VERSION),
        _m2_jar(
            "org/apache/arrow",
            "arrow-memory-netty-buffer-patch",
            ARROW_VERSION,
        ),
        _m2_jar("org/apache/arrow", "arrow-c-data", ARROW_VERSION),
        _m2_jar(
            "com/google/flatbuffers", "flatbuffers-java", FLATBUFFERS_VERSION
        ),
        # Spark ships protobuf-java 2.5.0 (sans MessageOrBuilder). The proto
        # surface in core (LogicalExprNode etc.) needs 3.25.x.
        _m2_jar("com/google/protobuf", "protobuf-java", PROTOBUF_VERSION),
    ]
    app_jars = [
        _resolve_jar("core", "datafusion-java"),
        _resolve_jar("spark", "datafusion-java-spark_2.13"),
        _resolve_jar("examples", "datafusion-java-examples"),
        *arrow_jars,
    ]
    jars = ",".join(app_jars)
    # Prepend the same jars onto the bootstrap classpath so Arrow 19's classes
    # are loaded by the system class loader — avoids the
    # ``UnsafeDirectLittleEndian cannot access superclass WrappedByteBuf``
    # IllegalAccessError that ChildFirstURLClassLoader produces when the
    # buffer-patch class lands in the child loader while Netty stays in the app
    # loader.
    extra_classpath = ":".join(app_jars)

    spark = (
        SparkSession.builder.appName("datafusion-ffi-demo")
        .master("local[2]")
        .config("spark.jars", jars)
        .config("spark.driver.extraClassPath", extra_classpath)
        .config("spark.executor.extraClassPath", extra_classpath)
        .config(
            "spark.driver.extraJavaOptions",
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
        )
        .config(
            "spark.executor.extraJavaOptions",
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
        )
        .getOrCreate()
    )

    # The example cdylib is bundled inside the examples jar and extracted by
    # NativeLibraryLoader at first use; no working-directory or path setup is
    # needed. (-Dexample.ffi.lib.path via extraJavaOptions overrides it for
    # unpackaged local builds.)

    # `name_prefix`, `num_rows`, `num_batches` are interpreted by
    # ExampleFfiProviderFactory.encodeOptions and decoded on the Rust side
    # in examples/native/src/lib.rs. They demonstrate driver-side options
    # flowing through to the native MemTable build.
    name_prefix = "user"
    num_rows = 5
    num_batches = 3
    df = (
        spark.read.format("datafusion")
        .option(
            "df.factory",
            "org.apache.datafusion.examples.ExampleFfiProviderFactory",
        )
        .option("name_prefix", name_prefix)
        .option("num_rows", str(num_rows))
        .option("num_batches", str(num_batches))
        .load()
    )

    total_rows = num_rows * num_batches
    print(f"=== options: name_prefix={name_prefix} num_rows={num_rows} num_batches={num_batches} ===")
    print(f"=== expecting {total_rows} rows across {num_batches} Arrow batches ===")

    print("=== schema ===")
    df.printSchema()

    print(f"=== full scan (first {total_rows} rows) ===")
    df.show(n=total_rows, truncate=False)

    print("=== filter pushdown: value > 5.0 ===")
    df.filter("value > 5.0").show(n=total_rows, truncate=False)

    print("=== projection: id, name ===")
    df.select("id", "name").show(n=total_rows, truncate=False)

    legacy_rows = {tuple(r) for r in df.collect()}

    # --- shared-scan mode -------------------------------------------------
    # `shared_scan=true` flips ExampleFfiProviderFactory.sharedScan: one
    # provider + plan cached per executor, one Spark task per MemTable
    # partition (num_partitions=4), each task streaming one DataFusion plan
    # partition. Results must be identical to the legacy run above.
    num_partitions = 4
    shared = (
        spark.read.format("datafusion")
        .option(
            "df.factory",
            "org.apache.datafusion.examples.ExampleFfiProviderFactory",
        )
        .option("name_prefix", name_prefix)
        .option("num_rows", str(num_rows))
        .option("num_batches", str(num_batches))
        .option("num_partitions", str(num_partitions))
        .option("shared_scan", "true")
        .load()
    )

    print(f"=== shared-scan mode: num_partitions={num_partitions} ===")
    shared_partitions = shared.rdd.getNumPartitions()
    print(f"=== shared-scan Spark partitions: {shared_partitions} ===")
    assert shared_partitions == num_partitions, (
        f"expected {num_partitions} Spark partitions in shared-scan mode, "
        f"got {shared_partitions}"
    )

    shared.show(n=total_rows, truncate=False)
    shared_rows = {tuple(r) for r in shared.collect()}
    assert shared_rows == legacy_rows, (
        "shared-scan rows diverge from legacy mode: "
        f"only-legacy={legacy_rows - shared_rows} only-shared={shared_rows - legacy_rows}"
    )
    print(f"=== shared-scan returned the same {len(shared_rows)} rows as legacy mode ===")

    print("=== shared-scan filter pushdown: value > 5.0 ===")
    shared.filter("value > 5.0").show(n=total_rows, truncate=False)

    # Note on cache scope: the executor cache is keyed by a per-query scanId,
    # so sharing happens across the TASKS of one query (4 tasks above -> one
    # provider build per executor JVM, observable via the factory's
    # createProvider stdout line), not across separate actions. Each new
    # action plans a new scan with a fresh scanId; its entry simply joins the
    # cache until the idle TTL evicts it.
    count_again = shared.count()
    assert count_again == total_rows, f"expected {total_rows} rows, got {count_again}"
    print("=== shared-scan count() as a separate action also succeeded ===")

    spark.stop()


if __name__ == "__main__":
    main()
