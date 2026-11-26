#!/usr/bin/env python3
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

"""End-to-end PySpark test of the adbc-datafusion connector + example driver.

PySpark drives the same JVM Spark, so the connector is used exactly as from
Scala/Java: spark.read.format("adbc-datafusion"). No Python-side connector code.

Prereqs:
  - arrow-adbc `main` (0.24.0-SNAPSHOT) built into ~/.m2 (Substrait + partitions),
  - the connector jar packaged (mvn -pl spark -Padbc-snapshot package),
  - its runtime deps copied to a dir (mvn dependency:copy-dependencies),
  - arrow-c-data (matching Spark's bundled Arrow, e.g. 18.1.0) added to that dir
    -- vanilla Spark bundles arrow-vector/arrow-memory but NOT arrow-c-data,
  - the example cdylib built (cargo build --release).

Configure via env vars (defaults assume this repo's build layout):
  CONNECTOR_JAR   path to datafusion-spark-*.jar
  ADBC_JARS_DIR   dir of runtime dep jars (adbc-*, substrait core, datafusion-java, ...)
  ADBC_DRIVER_LIB path to libadbc_datafusion_example_driver.{dylib,so}
"""

import datetime
import glob
import os
import sys
from decimal import Decimal

from pyspark.sql import SparkSession

# Field metadata flag the connector sets on columns it casts source-side to a Spark-native
# layout (SchemaConverter.CAST_METADATA_KEY).
CAST_META_KEY = "org.apache.datafusion.spark.adbc.cast"

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
LIBEXT = "dylib" if sys.platform == "darwin" else "so"

connector_jar = os.environ.get(
    "CONNECTOR_JAR",
    glob.glob(os.path.join(REPO, "spark/target/datafusion-spark-*.jar"))[0],
)
jars_dir = os.environ.get("ADBC_JARS_DIR", "/tmp/adbc-libs")
driver_lib = os.environ.get(
    "ADBC_DRIVER_LIB",
    os.path.join(REPO, f"rust-target/release/libadbc_datafusion_example_driver.{LIBEXT}"),
)

jars = [connector_jar] + glob.glob(os.path.join(jars_dir, "*.jar"))
assert os.path.exists(driver_lib), f"driver cdylib not found: {driver_lib}"

# Java 17 opens Spark/Arrow need (Spark 4.0 sets most; be explicit for the data path).
opens = " ".join(
    f"--add-opens=java.base/{p}=ALL-UNNAMED"
    for p in ("java.nio", "sun.nio.ch", "java.lang", "java.util")
)

spark = (
    SparkSession.builder.master("local[2]")
    .appName("adbc-datafusion-pyspark-e2e")
    .config("spark.jars", ",".join(jars))
    .config("spark.driver.extraJavaOptions", opens)
    .config("spark.executor.extraJavaOptions", opens)
    .getOrCreate()
)

def read(table):
    return (
        spark.read.format("adbc-datafusion")
        .option("driver", driver_lib)
        .option("entrypoint", "AdbcDatafusionExampleInit")
        .option("table", table)
        .load()
    )


try:
    # --- `example`: two Spark-native columns, three partitions -------------------------------
    df = read("example")

    print("=== example schema ===")
    df.printSchema()

    num_partitions = df.rdd.getNumPartitions()
    print("numPartitions:", num_partitions)

    ids = sorted(r["id"] for r in df.collect())
    names = sorted(r["name"] for r in df.select("name").collect())
    filtered = sorted(r["id"] for r in df.filter("id > 1").collect())
    print("full scan ids:", ids)
    print("projection names:", names)
    print("filter id>1:", filtered)

    assert ids == [1, 2, 3], ids
    assert names == ["alice", "bob", "carol"], names
    assert filtered == [2, 3], filtered
    assert num_partitions >= 2, f"expected multi-partition, got {num_partitions}"

    # --- `types`: the schema-conversion + source-side arrow_cast coverage --------------------
    dft = read("types")
    print("=== types schema ===")
    dft.printSchema()

    # cast columns are flagged (so filter pushdown stays off them); pass-through ones are not.
    cast_cols = {f.name for f in dft.schema.fields if f.metadata.get(CAST_META_KEY)}
    print("cast columns:", sorted(cast_cols))
    assert cast_cols == {
        "channel",
        "big",
        "event_time",
        "score",
        "tags",
        "vec",
        "labels",
        "digest",
        "day",
    }, cast_cols

    # count() prunes the projection to empty; the connector must not emit a bare SELECT * (which
    # would return the raw, uncast schema and fail the reader on the ns timestamp / unsigned ids).
    types_count = dft.count()
    print("types count:", types_count)
    assert types_count == 3, types_count

    # value correctness: collecting whole rows forces the vectorized reader to decode each
    # ArrowColumnVector, so a bad cast (wrong layout, unit relabel instead of rescale, unsigned
    # overflow) fails here.
    rows = {r["id"]: r for r in dft.collect()}
    r1, r2, r3 = rows[1], rows[2], rows[3]

    # unsigned UInt16 -> Integer, widened past i16::MAX
    assert [r1["channel"], r2["channel"], r3["channel"]] == [100, 40000, 65535]

    # unsigned UInt64 -> Decimal(20,0): lossless for values past i64::MAX (a Long would overflow)
    assert r1["big"] == Decimal("18446744073709551615")
    assert r2["big"] == Decimal(0)
    assert r3["big"] == Decimal("9223372036854775808")

    # nanosecond Timestamp -> microsecond TimestampNTZ, rescaled (not relabeled -> would be ~1970)
    assert r1["event_time"] == datetime.datetime(2020, 9, 13, 12, 26, 40)
    assert r2["event_time"] == datetime.datetime(2021, 1, 7, 6, 13, 20)
    assert r3["event_time"] == datetime.datetime(2021, 5, 3, 0, 0, 0)

    # Float16 -> Float
    assert [r1["score"], r2["score"], r3["score"]] == [1.5, 2.5, 3.5]

    # Binary passes through
    assert bytes(r1["payload"]) == b"\x01\x02"
    assert bytes(r2["payload"]) == b""
    assert bytes(r3["payload"]) == b"\xff\xfe"

    # nested List<UInt16> -> Array<Integer> (recursive cast)
    assert r1["tags"] == [1, 2]
    assert r2["tags"] == []
    assert r3["tags"] == [3]

    # FixedSizeList<UInt16> -> Array<Integer> (fixed->variable + element widening)
    assert r1["vec"] == [10, 20]
    assert r2["vec"] == [30, 40]
    assert r3["vec"] == [50, 60]

    # LargeList<Utf8> -> Array<String>
    assert r1["labels"] == ["a", "b"]
    assert r2["labels"] == []
    assert r3["labels"] == ["c"]

    # FixedSizeBinary -> Binary
    assert bytes(r1["digest"]) == b"\x01\x02\x03\x04"
    assert bytes(r3["digest"]) == b"\xff\xfe\xfd\xfc"

    # Date64 -> Date32 (day-aligned)
    assert r1["day"] == datetime.date(2020, 9, 13)
    assert r2["day"] == datetime.date(2021, 1, 7)
    assert r3["day"] == datetime.date(2021, 5, 3)

    # nested List<Struct<key,val>> passes through
    assert [(x["key"], x["val"]) for x in r1["attrs"]] == [("a", "1")]
    assert r2["attrs"] == []
    assert [(x["key"], x["val"]) for x in r3["attrs"]] == [("b", "2"), ("c", "3")]

    # --- filter on a cast column: excluded from pushdown, evaluated by Spark on the cast value -
    channel_gt = sorted(r["id"] for r in dft.filter(dft.channel > 100).collect())
    print("filter channel>100 ids:", channel_gt)
    assert channel_gt == [2, 3], channel_gt

    print("\nPYSPARK E2E OK (example: multi-partition + pushdown; types: casts + nested + filter)")
finally:
    spark.stop()
