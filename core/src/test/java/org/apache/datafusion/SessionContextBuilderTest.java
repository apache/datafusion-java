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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.datafusion.protobuf.ConfigOption;
import org.apache.datafusion.protobuf.SessionOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SessionContextBuilderTest {

  @Test
  void protoRoundTripPreservesAllFields() throws Exception {
    byte[] bytes =
        SessionContext.builder()
            .batchSize(8192)
            .targetPartitions(4)
            .collectStatistics(true)
            .informationSchema(true)
            .memoryLimit(1L << 30, 0.8)
            .tempDirectory("/tmp/df")
            .toBytes();

    SessionOptions parsed = SessionOptions.parseFrom(bytes);
    assertTrue(parsed.hasBatchSize());
    assertEquals(8192L, parsed.getBatchSize());
    assertTrue(parsed.hasTargetPartitions());
    assertEquals(4L, parsed.getTargetPartitions());
    assertTrue(parsed.hasCollectStatistics());
    assertTrue(parsed.getCollectStatistics());
    assertTrue(parsed.hasInformationSchema());
    assertTrue(parsed.getInformationSchema());
    assertTrue(parsed.hasMemoryLimit());
    assertEquals(1L << 30, parsed.getMemoryLimit().getMaxMemoryBytes());
    assertEquals(0.8, parsed.getMemoryLimit().getMemoryFraction(), 1e-9);
    assertTrue(parsed.hasTempDirectory());
    assertEquals("/tmp/df", parsed.getTempDirectory());
  }

  @Test
  void unsetFieldsAreAbsentInProto() throws Exception {
    byte[] bytes = SessionContext.builder().batchSize(8192).toBytes();
    SessionOptions parsed = SessionOptions.parseFrom(bytes);
    assertTrue(parsed.hasBatchSize());
    assertFalse(parsed.hasTargetPartitions());
    assertFalse(parsed.hasCollectStatistics());
    assertFalse(parsed.hasInformationSchema());
    assertFalse(parsed.hasMemoryLimit());
    assertFalse(parsed.hasTempDirectory());
  }

  @Test
  void informationSchemaEnabledMakesMetaQueryRun() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = SessionContext.builder().informationSchema(true).build();
        DataFrame df = ctx.sql("SELECT table_name FROM information_schema.tables");
        ArrowReader reader = df.collect(allocator)) {
      // information_schema.tables always has at least one row.
      assertTrue(reader.loadNextBatch());
    }
  }

  @Test
  void informationSchemaDisabledByDefaultThrows() {
    try (SessionContext ctx = SessionContext.builder().build()) {
      assertThrows(
          RuntimeException.class,
          () -> ctx.sql("SELECT table_name FROM information_schema.tables"));
    }
  }

  @Test
  void buildWithEveryKnobSetCanExecuteSelectOne(@TempDir Path tempDir) throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx =
            SessionContext.builder()
                .batchSize(8192)
                .targetPartitions(4)
                .collectStatistics(true)
                .informationSchema(true)
                .memoryLimit(1L << 30, 0.8)
                .tempDirectory(tempDir.toAbsolutePath().toString())
                .build();
        DataFrame df = ctx.sql("SELECT 1");
        ArrowReader reader = df.collect(allocator)) {
      assertTrue(reader.loadNextBatch());
      assertEquals(1, reader.getVectorSchemaRoot().getRowCount());
    }
  }

  @Test
  void setOptionRoundTripsThroughProto() throws Exception {
    byte[] bytes =
        SessionContext.builder()
            .setOption("datafusion.execution.parquet.pushdown_filters", "true")
            .setOption("datafusion.optimizer.prefer_hash_join", "true")
            .toBytes();
    SessionOptions parsed = SessionOptions.parseFrom(bytes);
    List<ConfigOption> opts = parsed.getOptionsList();
    assertEquals(2, opts.size());
    assertEquals("datafusion.execution.parquet.pushdown_filters", opts.get(0).getKey());
    assertEquals("true", opts.get(0).getValue());
    assertEquals("datafusion.optimizer.prefer_hash_join", opts.get(1).getKey());
    assertEquals("true", opts.get(1).getValue());
  }

  @Test
  void setOptionsBulkRoundTripsThroughProto() throws Exception {
    Map<String, String> entries = new LinkedHashMap<>();
    entries.put("datafusion.execution.time_zone", "UTC");
    entries.put("datafusion.optimizer.default_filter_selectivity", "10");
    byte[] bytes = SessionContext.builder().setOptions(entries).toBytes();
    SessionOptions parsed = SessionOptions.parseFrom(bytes);
    List<ConfigOption> opts = parsed.getOptionsList();
    assertEquals(2, opts.size());
    assertEquals("datafusion.execution.time_zone", opts.get(0).getKey());
    assertEquals("UTC", opts.get(0).getValue());
    assertEquals("datafusion.optimizer.default_filter_selectivity", opts.get(1).getKey());
    assertEquals("10", opts.get(1).getValue());
  }

  @Test
  void setOptionRejectsNullKeyOrValue() {
    SessionContextBuilder b = SessionContext.builder();
    assertThrows(IllegalArgumentException.class, () -> b.setOption(null, "v"));
    assertThrows(IllegalArgumentException.class, () -> b.setOption("k", null));
    assertThrows(IllegalArgumentException.class, () -> b.setOptions(null));
  }

  @Test
  void setOptionAffectsRunningSession() {
    try (SessionContext ctx =
        SessionContext.builder()
            .setOption("datafusion.execution.parquet.pushdown_filters", "true")
            .build()) {
      assertEquals("true", ctx.getOption("datafusion.execution.parquet.pushdown_filters"));
    }
  }

  @Test
  void setOptionLastWriteWinsWithinMap() throws Exception {
    byte[] bytes =
        SessionContext.builder()
            .setOption("datafusion.execution.batch_size", "1024")
            .setOption("datafusion.execution.batch_size", "8192")
            .toBytes();
    SessionOptions parsed = SessionOptions.parseFrom(bytes);
    // setOption dedups by key (remove-then-put), so only one entry reaches
    // the wire — the most-recently-written value, at the position of the
    // most recent setOption call.
    List<ConfigOption> opts = parsed.getOptionsList();
    assertEquals(1, opts.size());
    assertEquals("datafusion.execution.batch_size", opts.get(0).getKey());
    assertEquals("8192", opts.get(0).getValue());
  }

  @Test
  void setOptionOverridesTypedSetterAtConstruction() {
    // Typed setter says batch_size = 8192; setOption overrides to 1024.
    // The override is a real semantic change, not just absence of a throw —
    // assert that's what DataFusion is actually running with.
    try (SessionContext ctx =
        SessionContext.builder()
            .batchSize(8192)
            .setOption("datafusion.execution.batch_size", "1024")
            .build()) {
      assertEquals("1024", ctx.getOption("datafusion.execution.batch_size"));
    }
  }

  @Test
  void unknownOptionKeyThrowsAtBuild() {
    SessionContextBuilder b =
        SessionContext.builder().setOption("datafusion.execution.bogus_made_up_key", "x");
    RuntimeException thrown = assertThrows(RuntimeException.class, b::build);
    // The DataFusion error message includes the bad key text — assert it's
    // surfaced rather than swallowed.
    assertTrue(
        thrown.getMessage() != null && thrown.getMessage().contains("bogus_made_up_key"),
        "expected DataFusion error to mention bad key, got: " + thrown.getMessage());
  }

  @Test
  void getOptionReadsTypedSetterValue() {
    try (SessionContext ctx = SessionContext.builder().batchSize(4096).build()) {
      assertEquals("4096", ctx.getOption("datafusion.execution.batch_size"));
    }
  }

  @Test
  void getOptionWithNoExplicitValueReturnsDefault() {
    // batch_size has a non-null default in DataFusion, so the getter must
    // return the default rather than null when the user never set it.
    try (SessionContext ctx = SessionContext.builder().build()) {
      String batchSize = ctx.getOption("datafusion.execution.batch_size");
      assertTrue(
          batchSize != null && !batchSize.isEmpty(),
          "expected default batch_size, got: " + batchSize);
    }
  }

  @Test
  void getOptionRejectsNullKey() {
    try (SessionContext ctx = SessionContext.builder().build()) {
      assertThrows(IllegalArgumentException.class, () -> ctx.getOption(null));
    }
  }

  @Test
  void getOptionThrowsOnUnknownKey() {
    try (SessionContext ctx = SessionContext.builder().build()) {
      RuntimeException thrown =
          assertThrows(RuntimeException.class, () -> ctx.getOption("datafusion.no.such.key"));
      assertTrue(
          thrown.getMessage() != null && thrown.getMessage().contains("no.such.key"),
          "expected error to mention bad key, got: " + thrown.getMessage());
    }
  }

  @Test
  void getOptionThrowsAfterClose() {
    SessionContext ctx = SessionContext.builder().build();
    ctx.close();
    assertThrows(
        IllegalStateException.class, () -> ctx.getOption("datafusion.execution.batch_size"));
  }

  @Test
  void setOptionAppliesInInsertionOrder() throws Exception {
    // Some upstream setters have side effects on other keys. For example,
    // setting `datafusion.optimizer.enable_dynamic_filter_pushdown` rewrites
    // the per-operator `enable_*_dynamic_filter_pushdown` flags. The caller's
    // last write therefore must win. Pin the on-the-wire order; the Rust side
    // applies in this order and DataFusion's per-key setter sees the umbrella
    // first, the override second.
    byte[] bytes =
        SessionContext.builder()
            .setOption("datafusion.optimizer.enable_dynamic_filter_pushdown", "true")
            .setOption("datafusion.optimizer.enable_topk_dynamic_filter_pushdown", "false")
            .toBytes();
    SessionOptions parsed = SessionOptions.parseFrom(bytes);
    List<ConfigOption> opts = parsed.getOptionsList();
    assertEquals(2, opts.size());
    assertEquals("datafusion.optimizer.enable_dynamic_filter_pushdown", opts.get(0).getKey());
    assertEquals("datafusion.optimizer.enable_topk_dynamic_filter_pushdown", opts.get(1).getKey());

    // And on the running session, the topk override wins over the umbrella.
    try (SessionContext ctx =
        SessionContext.builder()
            .setOption("datafusion.optimizer.enable_dynamic_filter_pushdown", "true")
            .setOption("datafusion.optimizer.enable_topk_dynamic_filter_pushdown", "false")
            .build()) {
      assertEquals(
          "false", ctx.getOption("datafusion.optimizer.enable_topk_dynamic_filter_pushdown"));
      assertEquals("true", ctx.getOption("datafusion.optimizer.enable_dynamic_filter_pushdown"));
    }
  }

  @Test
  void setOptionRepeatedKeyMovesToEndOfIterationOrder() throws Exception {
    // Codex regression: a LinkedHashMap.put on an existing key updates the
    // value but keeps the *original* slot. If the caller writes
    //   topk=false  →  umbrella=true  →  topk=false (re-write)
    // the re-write of topk would otherwise stay at position 0, putting the
    // umbrella *after* it on the wire. The umbrella's setter has the side
    // effect of resetting topk back to true, silently undoing the override.
    // Pin both the wire-level order and the running-session behaviour.
    byte[] bytes =
        SessionContext.builder()
            .setOption("datafusion.optimizer.enable_topk_dynamic_filter_pushdown", "false")
            .setOption("datafusion.optimizer.enable_dynamic_filter_pushdown", "true")
            .setOption("datafusion.optimizer.enable_topk_dynamic_filter_pushdown", "false")
            .toBytes();
    SessionOptions parsed = SessionOptions.parseFrom(bytes);
    List<ConfigOption> opts = parsed.getOptionsList();
    assertEquals(2, opts.size());
    // Umbrella applies first; the re-issued topk override is last.
    assertEquals("datafusion.optimizer.enable_dynamic_filter_pushdown", opts.get(0).getKey());
    assertEquals("datafusion.optimizer.enable_topk_dynamic_filter_pushdown", opts.get(1).getKey());

    try (SessionContext ctx =
        SessionContext.builder()
            .setOption("datafusion.optimizer.enable_topk_dynamic_filter_pushdown", "false")
            .setOption("datafusion.optimizer.enable_dynamic_filter_pushdown", "true")
            .setOption("datafusion.optimizer.enable_topk_dynamic_filter_pushdown", "false")
            .build()) {
      assertEquals(
          "false", ctx.getOption("datafusion.optimizer.enable_topk_dynamic_filter_pushdown"));
    }
  }

  @Test
  void setOptionRejectsRuntimeKeysAtBuild() {
    // datafusion.runtime.* lives on a separate config object (RuntimeEnv) and
    // round-tripping through getOption/setOption has subtle correctness
    // pitfalls (lazy default-tempdir creation, integer K/M/G truncation,
    // OS-specific path separators). Reject runtime keys with a clear pointer
    // to the typed setters until a follow-up PR designs proper support.
    SessionContextBuilder b =
        SessionContext.builder().setOption("datafusion.runtime.memory_limit", "2G");
    RuntimeException thrown = assertThrows(RuntimeException.class, b::build);
    assertTrue(
        thrown.getMessage() != null
            && thrown.getMessage().contains("datafusion.runtime")
            && thrown.getMessage().contains("memoryLimit"),
        "expected runtime-key error to point at typed setters, got: " + thrown.getMessage());
  }

  @Test
  void getOptionRejectsRuntimeKeys() {
    try (SessionContext ctx = SessionContext.builder().build()) {
      RuntimeException thrown =
          assertThrows(
              RuntimeException.class, () -> ctx.getOption("datafusion.runtime.memory_limit"));
      assertTrue(
          thrown.getMessage() != null && thrown.getMessage().contains("datafusion.runtime"),
          "expected runtime-key error, got: " + thrown.getMessage());
    }
  }
}
