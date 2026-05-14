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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

class ScalarUdfTest {

  /** Returns null intentionally — only used to prove registration works without invoking. */
  static final class NeverCalled implements ScalarUdf {
    @Override
    public FieldVector evaluate(BufferAllocator allocator, List<FieldVector> args) {
      throw new AssertionError("should not be invoked in this test");
    }
  }

  @Test
  void registerUdf_succeedsAndResolvesByName() {
    try (SessionContext ctx = new SessionContext()) {
      ctx.registerUdf(
          "add_one",
          new NeverCalled(),
          new ArrowType.Int(32, true),
          List.of(new ArrowType.Int(32, true)),
          Volatility.IMMUTABLE);

      // The function is registered; planning a SQL query that references it should not throw
      // with "function not found". Execution would fail (invoke_with_args returns NotImplemented),
      // but we don't execute yet.
      try (DataFrame df = ctx.sql("SELECT add_one(CAST(1 AS INT))")) {
        assertNotNull(df);
      }
    }
  }

  @Test
  void invokingUnimplementedUdf_throwsNotImplemented() {
    try (SessionContext ctx = new SessionContext();
        BufferAllocator allocator = new org.apache.arrow.memory.RootAllocator()) {
      ctx.registerUdf(
          "add_one",
          new NeverCalled(),
          new ArrowType.Int(32, true),
          List.of(new ArrowType.Int(32, true)),
          Volatility.IMMUTABLE);

      // Until Task 6 lands, invoke_with_args returns NotImplemented; collect should fail.
      RuntimeException ex =
          assertThrows(
              RuntimeException.class,
              () -> {
                try (DataFrame df = ctx.sql("SELECT add_one(CAST(1 AS INT))");
                    org.apache.arrow.vector.ipc.ArrowReader r = df.collect(allocator)) {
                  while (r.loadNextBatch()) {
                    // drain
                  }
                }
              });
      // The test asserts the error path is reached; exact message is checked in Task 6 once
      // invoke is implemented and this test is replaced by a real assertion.
      assertNotNull(ex.getMessage());
    }
  }
}
