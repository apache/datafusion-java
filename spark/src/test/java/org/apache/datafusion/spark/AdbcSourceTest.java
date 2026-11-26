/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datafusion.spark;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.arrow.adbc.core.TypedKey;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * End-to-end test of the {@code adbc-datafusion} source against the example driver cdylib (built
 * from {@code examples/adbc-datafusion-driver}). It drives the full stack: Spark DataSourceV2 ->
 * arrow-adbc JNI driver manager -> the DataFusion ADBC cdylib -> our custom provider.
 *
 * <p>Requires the cdylib path in the {@code adbc.example.driver.path} system property; the test is
 * skipped (not failed) when it is absent, so the build is green without the native artifact. To
 * run:
 *
 * <pre>
 *   (cd examples/adbc-datafusion-driver &amp;&amp; cargo build --release)
 *   mvn -pl spark -am test -Dtest=AdbcSourceTest \
 *     -Dadbc.example.driver.path=$PWD/rust-target/release/libadbc_datafusion_example_driver.dylib
 * </pre>
 */
class AdbcSourceTest {

  private static final String ENTRYPOINT = "AdbcDatafusionExampleInit";
  private static final String TABLE = "example";
  // A second table whose schema spans the Arrow types the connector maps or casts.
  private static final String TYPES_TABLE = "types";

  private static SparkSession spark;
  private static String driverPath;

  @BeforeAll
  static void setUp() {
    driverPath = System.getProperty("adbc.example.driver.path");
    assumeTrue(
        driverPath != null && Files.exists(Path.of(driverPath)),
        "set -Dadbc.example.driver.path to the example driver cdylib to run this test");
    // local[8]: several task slots in one executor JVM, so the per-executor connection cache
    // (AdbcConnectionPool) is exercised across concurrent tasks.
    // Java 8 date/time API: DateType -> java.time.LocalDate, avoiding Spark's legacy
    // java.sql.Date conversion which needs sun.util.calendar opened on JDK 17.
    spark =
        SparkSession.builder()
            .appName("adbc-source-test")
            .master("local[8]")
            .config("spark.sql.datetime.java8API.enabled", "true")
            .getOrCreate();
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  private Dataset<Row> load() {
    return load(TABLE);
  }

  private Dataset<Row> load(String table) {
    return spark
        .read()
        .format("adbc-datafusion")
        .option("driver", driverPath)
        .option("entrypoint", ENTRYPOINT)
        .option("table", table)
        .load();
  }

  /**
   * The {@code types} table exercises the full converter: directly-representable columns pass
   * through, cast-required columns (unsigned, ns timestamp, Float16, nested {@code List<UInt16>})
   * are cast to a Spark-native layout by a source-side {@code arrow_cast} pushed into the scan.
   * This asserts the reported Spark types and the cast-column metadata flags.
   */
  @Test
  void typesSchemaMapsAndFlagsCasts() {
    StructType schema = load(TYPES_TABLE).schema();

    assertEquals(DataTypes.BinaryType, schema.apply("payload").dataType());
    assertEquals(DataTypes.IntegerType, schema.apply("channel").dataType());
    assertEquals(DataTypes.createDecimalType(20, 0), schema.apply("big").dataType());
    assertEquals(DataTypes.TimestampNTZType, schema.apply("event_time").dataType());
    assertEquals(DataTypes.FloatType, schema.apply("score").dataType());
    assertEquals(
        DataTypes.createArrayType(DataTypes.IntegerType, true), schema.apply("tags").dataType());
    // FixedSizeList<UInt16> -> variable Array<Integer> (fixed layout not readable by Spark).
    assertEquals(
        DataTypes.createArrayType(DataTypes.IntegerType, true), schema.apply("vec").dataType());
    // LargeList<Utf8> -> Array<String>; FixedSizeBinary -> Binary; Date64 -> Date.
    assertEquals(
        DataTypes.createArrayType(DataTypes.StringType, true), schema.apply("labels").dataType());
    assertEquals(DataTypes.BinaryType, schema.apply("digest").dataType());
    assertEquals(DataTypes.DateType, schema.apply("day").dataType());

    // Cast columns are flagged (so filter pushdown stays off them); pass-through columns are not.
    assertTrue(schema.apply("channel").metadata().contains(SchemaConverter.CAST_METADATA_KEY));
    assertTrue(schema.apply("big").metadata().contains(SchemaConverter.CAST_METADATA_KEY));
    assertTrue(schema.apply("event_time").metadata().contains(SchemaConverter.CAST_METADATA_KEY));
    assertTrue(schema.apply("score").metadata().contains(SchemaConverter.CAST_METADATA_KEY));
    assertTrue(schema.apply("tags").metadata().contains(SchemaConverter.CAST_METADATA_KEY));
    assertTrue(schema.apply("vec").metadata().contains(SchemaConverter.CAST_METADATA_KEY));
    assertTrue(schema.apply("labels").metadata().contains(SchemaConverter.CAST_METADATA_KEY));
    assertTrue(schema.apply("digest").metadata().contains(SchemaConverter.CAST_METADATA_KEY));
    assertTrue(schema.apply("day").metadata().contains(SchemaConverter.CAST_METADATA_KEY));
    assertFalse(schema.apply("payload").metadata().contains(SchemaConverter.CAST_METADATA_KEY));
    assertFalse(schema.apply("attrs").metadata().contains(SchemaConverter.CAST_METADATA_KEY));
  }

  /**
   * The cast-requiring scan still parallelizes. Casts force the SQL wire (Substrait can't encode
   * unsigned/Float16), but ADBC {@code executePartitioned} is wire-independent -- it partitions on
   * the physical plan's output partitioning, and the source-side {@code arrow_cast} is a
   * partition-preserving projection -- so the {@code types} scan gets one Spark partition per
   * driver partition, exactly like the Substrait-wire {@code example} scan.
   */
  @Test
  void typesScanIsMultiPartitionDespiteCasts() {
    assertTrue(
        load(TYPES_TABLE).rdd().getNumPartitions() >= 2,
        "expected multiple Spark partitions for the cast (SQL-wire) scan, got "
            + load(TYPES_TABLE).rdd().getNumPartitions());
  }

  /**
   * count() over a cast-requiring table. Catalyst prunes the projection to empty for a count, and a
   * bare {@code SELECT *} would return the raw (uncast) schema -- the reader would then reject a
   * non-Spark-native column (e.g. {@code Timestamp(NANOSECOND)}). The connector emits a single
   * readable probe column instead, so the row count still comes back.
   */
  @Test
  void typesCountWorksWithoutMaterializingCastColumns() {
    assertEquals(3L, load(TYPES_TABLE).count());
  }

  /** Every column decodes to the expected Spark-native value -- casts are value-correct. */
  @Test
  void typesValuesRoundTripThroughCasts() {
    Map<Long, Row> byId =
        load(TYPES_TABLE).collectAsList().stream()
            .collect(Collectors.toMap(r -> r.getLong(r.fieldIndex("id")), r -> r));
    assertEquals(Set.of(1L, 2L, 3L), byId.keySet());
    Row r1 = byId.get(1L);
    Row r2 = byId.get(2L);
    Row r3 = byId.get(3L);

    // unsigned UInt16 -> Integer, widened past i16::MAX.
    assertEquals(100, r1.getInt(r1.fieldIndex("channel")));
    assertEquals(40_000, r2.getInt(r2.fieldIndex("channel")));
    assertEquals(65_535, r3.getInt(r3.fieldIndex("channel")));

    // unsigned UInt64 -> Decimal(20,0): lossless for values past i64::MAX (a Long would overflow).
    assertEquals(0, new BigDecimal("18446744073709551615").compareTo(bigValue(r1)));
    assertEquals(0, BigDecimal.ZERO.compareTo(bigValue(r2)));
    assertEquals(0, new BigDecimal("9223372036854775808").compareTo(bigValue(r3)));

    // nanosecond Timestamp -> microsecond TimestampNTZ, rescaled (a relabel would land near 1970).
    assertEquals(LocalDateTime.of(2020, 9, 13, 12, 26, 40), r1.getAs("event_time"));
    assertEquals(LocalDateTime.of(2021, 1, 7, 6, 13, 20), r2.getAs("event_time"));
    assertEquals(LocalDateTime.of(2021, 5, 3, 0, 0, 0), r3.getAs("event_time"));

    // Float16 -> Float.
    assertEquals(1.5f, r1.getFloat(r1.fieldIndex("score")));
    assertEquals(2.5f, r2.getFloat(r2.fieldIndex("score")));

    // Binary passes through.
    assertArrayEquals(new byte[] {0x01, 0x02}, (byte[]) r1.getAs("payload"));
    assertArrayEquals(new byte[] {}, (byte[]) r2.getAs("payload"));
    assertArrayEquals(new byte[] {(byte) 0xff, (byte) 0xfe}, (byte[]) r3.getAs("payload"));

    // nested List<UInt16> -> Array<Integer> (recursive cast).
    assertEquals(List.of(1, 2), r1.getList(r1.fieldIndex("tags")));
    assertEquals(List.of(), r2.getList(r2.fieldIndex("tags")));
    assertEquals(List.of(3), r3.getList(r3.fieldIndex("tags")));

    // FixedSizeList<UInt16> -> Array<Integer> (fixed->variable + element widening).
    assertEquals(List.of(10, 20), r1.getList(r1.fieldIndex("vec")));
    assertEquals(List.of(30, 40), r2.getList(r2.fieldIndex("vec")));
    assertEquals(List.of(50, 60), r3.getList(r3.fieldIndex("vec")));

    // LargeList<Utf8> -> Array<String> (large list not readable -> cast to variable list).
    assertEquals(List.of("a", "b"), r1.getList(r1.fieldIndex("labels")));
    assertEquals(List.of(), r2.getList(r2.fieldIndex("labels")));
    assertEquals(List.of("c"), r3.getList(r3.fieldIndex("labels")));

    // FixedSizeBinary -> Binary.
    assertArrayEquals(new byte[] {0x01, 0x02, 0x03, 0x04}, (byte[]) r1.getAs("digest"));
    assertArrayEquals(
        new byte[] {(byte) 0xff, (byte) 0xfe, (byte) 0xfd, (byte) 0xfc},
        (byte[]) r3.getAs("digest"));

    // Date64 -> Date32 (day-aligned).
    assertEquals(LocalDate.ofEpochDay(18518), toLocalDate(r1.getAs("day")));
    assertEquals(LocalDate.ofEpochDay(18634), toLocalDate(r2.getAs("day")));
    assertEquals(LocalDate.ofEpochDay(18750), toLocalDate(r3.getAs("day")));

    // nested List<Struct<key,val>> passes through.
    List<Row> attrs3 = r3.getList(r3.fieldIndex("attrs"));
    assertEquals(2, attrs3.size());
    assertEquals("b", attrs3.get(0).getAs("key"));
    assertEquals("2", attrs3.get(0).getAs("val"));
    assertEquals("c", attrs3.get(1).getAs("key"));
    assertEquals(List.of(), r2.getList(r2.fieldIndex("attrs")));
  }

  private static BigDecimal bigValue(Row row) {
    return row.getDecimal(row.fieldIndex("big"));
  }

  /** Spark returns DateType as java.sql.Date, or java.time.LocalDate under the Java 8 date API. */
  private static LocalDate toLocalDate(Object value) {
    return value instanceof LocalDate d ? d : ((java.sql.Date) value).toLocalDate();
  }

  @Test
  void fullScanAcrossPartitions() {
    Dataset<Row> df = load();

    // Schema inferred via ADBC getTableSchema.
    assertEquals("id", df.schema().fields()[0].name());
    assertEquals("name", df.schema().fields()[1].name());

    // The provider has three partitions; with arrow-adbc >= 0.24 (executePartitioned
    // forwarded by the JNI bridge) they surface as multiple Spark input partitions.
    assertTrue(
        df.rdd().getNumPartitions() >= 2,
        "expected multiple Spark partitions, got " + df.rdd().getNumPartitions());

    List<Long> ids =
        df.collectAsList().stream().map(r -> r.getLong(0)).sorted().collect(Collectors.toList());
    assertEquals(List.of(1L, 2L, 3L), ids);
  }

  @Test
  void projectionPushdown() {
    List<String> columns =
        load().select("name").collectAsList().stream()
            .map(r -> r.getString(0))
            .sorted()
            .collect(Collectors.toList());
    assertEquals(List.of("alice", "bob", "carol"), columns);
  }

  @Test
  void filterPushdown() {
    List<Long> ids =
        load().filter("id > 1").collectAsList().stream()
            .map(r -> r.getLong(0))
            .sorted()
            .collect(Collectors.toList());
    assertEquals(List.of(2L, 3L), ids);
  }

  /**
   * Regression for the per-executor cache: across a multi-partition scan over many task slots, the
   * pool builds the native database (and so runs the driver's {@code ContextInit} provider
   * registration) exactly once per executor JVM -- not once per task. Each task still opens its own
   * connection off that one cached database.
   */
  @Test
  void providerRegisteredOncePerExecutor() {
    // Force reader tasks to run across the executor's slots.
    long rows = load().collectAsList().size();
    assertTrue(rows > 0, "scan returned rows");

    // All tasks share one driver+options key, so the pool builds exactly one native database for
    // this executor JVM regardless of partition/task count.
    assertEquals(
        1,
        AdbcConnectionPool.databasesBuiltForTesting(),
        "expected one native database per executor JVM, got "
            + AdbcConnectionPool.databasesBuiltForTesting());

    // One cached database, but each task opens its own connection off it.
    assertTrue(
        AdbcConnectionPool.taskConnectionsOpenedForTesting() > 0,
        "expected a per-task connection off the one cached database");
  }

  /**
   * Proves the driver's database-scoped plan cache is actually used. Every partition descriptor of
   * one query carries the same serialized physical plan; the driver deserializes it once and caches
   * it across all connections opened from the (per-executor) cached database. So the {@code N}
   * per-task connections of one scan must trigger exactly <b>one</b> deserialize, not {@code N}.
   *
   * <p>The driver exposes its deserialize count as the read-only int option {@code
   * adbc.datafusion.plan_deserialize_count}. The count is JVM-lifetime and shared across tests, so
   * we assert on the delta around a single scan, and use a distinct predicate so the plan bytes are
   * fresh (not already cached by another test) -- making the expected delta exactly 1.
   */
  @Test
  void planDeserializedOncePerExecutorNotPerTask() throws Exception {
    TypedKey<Long> deserializeCount =
        new TypedKey<>("adbc.datafusion.plan_deserialize_count", Long.class);

    // Distinct predicate -> distinct physical plan -> plan bytes not cached by another test.
    Dataset<Row> df = load().filter("id >= 1");
    int partitions = df.rdd().getNumPartitions();
    assertTrue(partitions >= 2, "need multiple partitions to test the collapse, got " + partitions);

    // Same options the scan uses -> same pool cache key -> same cached database (and counter).
    LinkedHashMap<String, String> raw = new LinkedHashMap<>();
    raw.put("driver", driverPath);
    raw.put("entrypoint", ENTRYPOINT);
    raw.put("table", TABLE);
    AdbcOptions opts = AdbcOptions.fromOptions(new CaseInsensitiveStringMap(raw));

    long before;
    try (AdbcConnectionPool.Lease lease = AdbcConnectionPool.acquire(opts)) {
      before = lease.connection().getOption(deserializeCount);
    }

    df.collectAsList(); // runs read_partition on `partitions` separate task connections

    long after;
    try (AdbcConnectionPool.Lease lease = AdbcConnectionPool.acquire(opts)) {
      after = lease.connection().getOption(deserializeCount);
    }

    assertEquals(
        1L,
        after - before,
        "the "
            + partitions
            + " partitions of one query must share ONE plan deserialize (database-scoped cache); "
            + "a per-connection cache would deserialize once per task = "
            + partitions);
  }
}
