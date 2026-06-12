#!/usr/bin/env python3
"""Smoke test: scan the __FORMAT__ bridge's demo table through PySpark.

Prerequisites:
  - cargo build --manifest-path native/Cargo.toml   (the bridge cdylib)
  - mvn package                                     (the shaded jar)
  - a Scala 2.13 Spark distribution; the PyPI pyspark wheel embeds 2.12, so
    point SPARK_HOME at e.g. spark-3.5.7-bin-hadoop3-scala2.13.

Run:  python3 smoke_test.py
"""

import glob
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent

spark_home = os.environ.get("SPARK_HOME")
if not spark_home or not Path(spark_home, "jars").is_dir():
    sys.exit("Set SPARK_HOME to a Scala 2.13 Spark distribution.")
os.environ["SPARK_HOME"] = spark_home

jars = glob.glob(str(PROJECT_ROOT / "target" / "__CRATE__-*.jar"))
jars = [j for j in jars if not j.endswith(("-sources.jar", "-javadoc.jar"))]
if not jars:
    sys.exit("Shaded jar not found under target/. Run 'mvn package' first.")
jar = jars[0]

from pyspark.sql import SparkSession  # noqa: E402

spark = (
    SparkSession.builder.appName("__FORMAT__-smoke")
    .master("local[2]")
    .config("spark.jars", jar)
    # extraClassPath PREPENDS, so the fat jar's Arrow wins over Spark's
    # bundled (older) copy on both driver and executors.
    .config("spark.driver.extraClassPath", jar)
    .config("spark.executor.extraClassPath", jar)
    .config("spark.driver.extraJavaOptions", "--add-opens=java.base/java.nio=ALL-UNNAMED")
    .config("spark.executor.extraJavaOptions", "--add-opens=java.base/java.nio=ALL-UNNAMED")
    .getOrCreate()
)

df = spark.read.format("__FORMAT__").option("rows", "5").load()
df.printSchema()
df.show(truncate=False)
count = df.count()
filtered = df.filter("id >= 2").count()
spark.stop()

assert count == 5, f"expected 5 rows, got {count}"
assert filtered == 3, f"expected 3 rows with id >= 2, got {filtered}"
print("smoke test OK: 5 rows scanned, filter returned 3")
