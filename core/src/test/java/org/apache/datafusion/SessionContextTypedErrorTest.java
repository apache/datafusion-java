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

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * End-to-end checks that real native call sites surface as the right typed Java exception. The
 * assertions are on exception <em>type</em>, not message text — the whole point of the typed
 * hierarchy is to free callers from message-string sniffing.
 */
class SessionContextTypedErrorTest {

  @Test
  void unknownTableInSqlIsPlanException() {
    try (SessionContext ctx = new SessionContext()) {
      DataFusionException e =
          assertThrows(
              DataFusionException.class, () -> ctx.sql("SELECT * FROM nonexistent_table_xyz_42"));
      assertInstanceOf(PlanException.class, e);
    }
  }

  @Test
  void malformedParquetFileIsIoException(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir)
      throws java.io.IOException {
    // Triggering a clean DataFusionError::IoError / ParquetError requires a
    // file that exists but isn't a valid parquet -- a missing path is
    // classified as "no table paths" (Execution) rather than IoError.
    java.nio.file.Path bogus = dir.resolve("not-actually-parquet.parquet");
    java.nio.file.Files.writeString(bogus, "this is not a parquet file");
    try (SessionContext ctx = new SessionContext()) {
      DataFusionException e =
          assertThrows(
              DataFusionException.class,
              () -> ctx.registerParquet("t", bogus.toAbsolutePath().toString()));
      assertInstanceOf(IoException.class, e);
    }
  }

  @Test
  void unknownConfigKeyIsConfigurationException() {
    // SessionContextBuilder.setOption defers config-key validation to build()
    // time, where DataFusion's `ConfigOptions::set` returns
    // DataFusionError::Configuration for unrecognised keys.
    SessionContextBuilder b =
        SessionContext.builder().setOption("datafusion.bogus_made_up_key_for_typed_test", "x");
    DataFusionException e = assertThrows(DataFusionException.class, b::build);
    assertInstanceOf(ConfigurationException.class, e);
  }

  @Test
  void divideByZeroIsExecutionException() throws java.io.IOException {
    // SELECT 1/0 surfaces as DataFusionError::ArrowError(ArrowError::DivideByZero)
    // -- the case Codex flagged that previously routed to IoException because
    // the outer arm matched *any* ArrowError. ExecutionException is the right
    // typed class for an arithmetic failure produced during query execution.
    try (SessionContext ctx = new SessionContext();
        org.apache.arrow.memory.RootAllocator allocator =
            new org.apache.arrow.memory.RootAllocator()) {
      DataFusionException e =
          assertThrows(
              DataFusionException.class,
              () -> {
                try (DataFrame df = ctx.sql("SELECT 1/0")) {
                  df.collect(allocator).close();
                }
              });
      assertInstanceOf(ExecutionException.class, e);
    }
  }

  @Test
  void runtimeKeyRejectionIsParentDataFusionException() {
    // The native-side guard against `datafusion.runtime.*` keys uses a
    // stringly-typed `Err("...")?` (not a DataFusionError variant), so it
    // surfaces as the parent DataFusionException — the catch-all path that
    // protects callers from a wrong typed subclass when the underlying error
    // doesn't carry a clean variant signal.
    SessionContextBuilder b =
        SessionContext.builder().setOption("datafusion.runtime.memory_limit", "2G");
    DataFusionException e = assertThrows(DataFusionException.class, b::build);
    // Strictly the parent class, not any subclass.
    if (e instanceof PlanException
        || e instanceof IoException
        || e instanceof ConfigurationException
        || e instanceof ExecutionException
        || e instanceof ResourcesExhaustedException
        || e instanceof NotImplementedException) {
      throw new AssertionError(
          "expected parent DataFusionException, got " + e.getClass().getName());
    }
  }
}
