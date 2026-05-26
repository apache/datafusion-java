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

package org.apache.datafusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelRoot;
import io.substrait.proto.Type;
import io.substrait.proto.Version;

/**
 * Round-trip tests for {@link SessionContext#fromSubstrait(byte[])}. Plans are constructed by hand
 * via the protobuf-Java classes shipped by {@code io.substrait:core} (test scope) — no external
 * SQL→Substrait compiler is required.
 *
 * <p>The {@code substrait} Cargo feature is off by default in {@code native/Cargo.toml}; if the
 * native crate was built without it, every test here is skipped (see {@link #checkFeatureEnabled}).
 * Run {@code (cd native && cargo build --features substrait)} before {@code ./mvnw test} to
 * exercise this class.
 */
class SessionContextSubstraitTest {

  @BeforeAll
  static void checkFeatureEnabled() {
    // The native crate's feature-off stub returns this exact error; everything else (including a
    // prost decode failure on garbage bytes) means the feature is compiled in.
    try (SessionContext ctx = new SessionContext()) {
      ctx.fromSubstrait(new byte[] {0});
      // fromSubstrait should never succeed on a single zero byte — but if it ever does, the
      // feature is clearly compiled in.
    } catch (RuntimeException e) {
      String msg = e.getMessage() == null ? "" : e.getMessage();
      Assumptions.assumeFalse(
          msg.contains("substrait` Cargo feature"),
          "datafusion-jni built without the `substrait` feature; skipping Substrait tests");
    }
  }

  /**
   * Build a minimal Substrait {@code Plan} that scans a registered named table {@code tableName}
   * with two columns {@code (id int32, v int32)} and projects them through unchanged.
   */
  private static Plan namedTableScanPlan(String tableName) {
    NamedStruct schema =
        NamedStruct.newBuilder()
            .addNames("id")
            .addNames("v")
            .setStruct(
                Type.Struct.newBuilder()
                    .addTypes(
                        Type.newBuilder()
                            .setI64(
                                Type.I64
                                    .newBuilder()
                                    .setNullability(Type.Nullability.NULLABILITY_REQUIRED)))
                    .addTypes(
                        Type.newBuilder()
                            .setI64(
                                Type.I64
                                    .newBuilder()
                                    .setNullability(Type.Nullability.NULLABILITY_REQUIRED)))
                    .setNullability(Type.Nullability.NULLABILITY_REQUIRED))
            .build();
    ReadRel read =
        ReadRel.newBuilder()
            .setBaseSchema(schema)
            .setNamedTable(ReadRel.NamedTable.newBuilder().addNames(tableName))
            .build();
    Rel rel = Rel.newBuilder().setRead(read).build();
    RelRoot root = RelRoot.newBuilder().setInput(rel).addNames("id").addNames("v").build();
    return Plan.newBuilder()
        .setVersion(Version.newBuilder().setMinorNumber(62))
        .addRelations(PlanRel.newBuilder().setRoot(root))
        .build();
  }

  /** Write {@code id,v} CSV to a temp file with the given int rows and return the path. */
  private static Path writeCsv(Path dir, String name, int[][] rows) {
    try {
      Path csv = dir.resolve(name);
      StringBuilder sb = new StringBuilder("id,v\n");
      for (int[] r : rows) {
        sb.append(r[0]).append(',').append(r[1]).append('\n');
      }
      Files.writeString(csv, sb.toString(), StandardCharsets.UTF_8);
      return csv;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Test
  void executesScanOfRegisteredTable(@TempDir Path tempDir) {
    Path csv = writeCsv(tempDir, "t.csv", new int[][] {{1, 10}, {2, 20}, {3, 30}});
    try (SessionContext ctx = new SessionContext()) {
      ctx.registerCsv("t", csv.toAbsolutePath().toString());
      Plan plan = namedTableScanPlan("t");
      try (DataFrame df = ctx.fromSubstrait(plan.toByteArray())) {
        assertEquals(3L, df.count());
      }
    }
  }

  @Test
  void scanResultIsLazyDataFrame(@TempDir Path tempDir) {
    // The DataFrame returned by fromSubstrait is a lazy plan, just like ctx.sql(...) — we can
    // chain transformations against it before executing.
    Path csv = writeCsv(tempDir, "t.csv", new int[][] {{1, 10}, {2, 20}, {3, 30}, {4, 40}});
    try (SessionContext ctx = new SessionContext()) {
      ctx.registerCsv("t", csv.toAbsolutePath().toString());
      byte[] bytes = namedTableScanPlan("t").toByteArray();
      try (DataFrame scanned = ctx.fromSubstrait(bytes);
          DataFrame filtered = scanned.filter("v >= 20")) {
        assertEquals(3L, filtered.count());
      }
    }
  }

  @Test
  void unknownTableThrows() {
    try (SessionContext ctx = new SessionContext()) {
      // Not registered.
      Plan plan = namedTableScanPlan("missing_table");
      RuntimeException e =
          assertThrows(RuntimeException.class, () -> ctx.fromSubstrait(plan.toByteArray()));
      // The DataFusion side surfaces a "table not found" / equivalent error message; match
      // loosely so the test isn't brittle to upstream wording changes.
      assertTrue(
          e.getMessage().toLowerCase().contains("missing_table")
              || e.getMessage().toLowerCase().contains("not found")
              || e.getMessage().toLowerCase().contains("table"),
          () -> "unexpected error message: " + e.getMessage());
    }
  }

  @Test
  void invalidBytesThrow() {
    try (SessionContext ctx = new SessionContext()) {
      // A short non-protobuf byte sequence — prost's decode will reject this.
      byte[] garbage = new byte[] {(byte) 0xFF, (byte) 0xFE, 0x01, 0x02, 0x03};
      assertThrows(RuntimeException.class, () -> ctx.fromSubstrait(garbage));
    }
  }

  @Test
  void emptyBytesThrow() {
    try (SessionContext ctx = new SessionContext()) {
      // An empty Plan has no relations; the consumer rejects it.
      byte[] emptyPlan = Plan.newBuilder().build().toByteArray();
      assertThrows(RuntimeException.class, () -> ctx.fromSubstrait(emptyPlan));
    }
  }

  @Test
  void nullBytesThrowIllegalArgument() {
    try (SessionContext ctx = new SessionContext()) {
      assertThrows(IllegalArgumentException.class, () -> ctx.fromSubstrait(null));
    }
  }

  @Test
  void closedContextThrows() {
    SessionContext ctx = new SessionContext();
    ctx.close();
    assertThrows(IllegalStateException.class, () -> ctx.fromSubstrait(new byte[] {0}));
  }
}
