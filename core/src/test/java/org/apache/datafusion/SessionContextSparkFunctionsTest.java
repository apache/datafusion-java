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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.Test;

class SessionContextSparkFunctionsTest {

  // crc32 is a Spark function provided by datafusion-spark and is NOT a core
  // DataFusion built-in. Spark's crc32('Spark') == 1557323817 (standard
  // CRC-32 of the ASCII bytes), so it doubles as a correctness check.
  @Test
  void sparkFunctionIsCallableWhenEnabled() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = SessionContext.builder().withSparkFunctions().build();
        DataFrame df = ctx.sql("SELECT crc32('Spark') AS c");
        ArrowReader reader = df.collect(allocator)) {
      assertTrue(reader.loadNextBatch());
      BigIntVector v = (BigIntVector) reader.getVectorSchemaRoot().getVector("c");
      assertEquals(1, v.getValueCount());
      assertEquals(1557323817L, v.get(0));
    }
  }

  // Without the flag, crc32 is unregistered, so planning the same SQL must
  // fail. This is the proof that withSparkFunctions() is what enabled it.
  @Test
  void sparkFunctionIsAbsentByDefault() {
    try (SessionContext ctx = SessionContext.builder().build()) {
      RuntimeException thrown =
          assertThrows(RuntimeException.class, () -> ctx.sql("SELECT crc32('Spark')"));
      assertTrue(
          thrown.getMessage() != null && thrown.getMessage().toLowerCase().contains("crc32"),
          "expected error to mention the unknown function, got: " + thrown.getMessage());
    }
  }
}
