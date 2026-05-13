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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
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
}
