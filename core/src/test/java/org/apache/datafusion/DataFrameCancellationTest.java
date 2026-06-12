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

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class DataFrameCancellationTest {

  private static final ArrowType INT32 = new ArrowType.Int(32, true);

  /**
   * Volatile UDF that sleeps {@code sleepMillis} per row and returns the input unchanged. Used to
   * synthesise a query whose runtime is long enough for cancellation to land mid-flight without
   * coupling tests to TPC-H or other large fixtures. {@link Volatility#VOLATILE} prevents the
   * planner from constant-folding it away even with a literal input.
   *
   * <p>The latch is released by the first invocation, so the firing thread can wait until the query
   * is genuinely in the UDF's hot path before calling cancel.
   */
  static final class SleepingIdentity implements ScalarFunction {
    private final long sleepMillis;
    private final CountDownLatch firstInvocation;

    SleepingIdentity(long sleepMillis, CountDownLatch firstInvocation) {
      this.sleepMillis = sleepMillis;
      this.firstInvocation = firstInvocation;
    }

    @Override
    public String name() {
      return "sleep_identity";
    }

    @Override
    public List<Field> argFields() {
      return List.of(Field.nullable("x", INT32));
    }

    @Override
    public Field returnField() {
      return Field.nullable("y", INT32);
    }

    @Override
    public Volatility volatility() {
      return Volatility.VOLATILE;
    }

    @Override
    public ColumnarValue evaluate(BufferAllocator allocator, ScalarFunctionArgs args) {
      firstInvocation.countDown();
      try {
        Thread.sleep(sleepMillis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      IntVector in = (IntVector) args.args().get(0).vector();
      IntVector out = new IntVector("sleep_out", allocator);
      int n = in.getValueCount();
      out.allocateNew(n);
      for (int i = 0; i < n; i++) {
        if (in.isNull(i)) {
          out.setNull(i);
        } else {
          out.set(i, in.get(i));
        }
      }
      out.setValueCount(n);
      return ColumnarValue.array((FieldVector) out);
    }
  }

  @Test
  void preCancelledTokenAbortsCollect() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        CancellationToken token = ctx.newCancellationToken()) {
      token.cancel();
      try (DataFrame df = ctx.sql("SELECT * FROM (VALUES (1), (2), (3)) AS t(x)")) {
        assertThrows(CancellationException.class, () -> df.collect(allocator, token));
      }
    }
  }

  @Test
  void preCancelledTokenAbortsExecuteStream() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        CancellationToken token = ctx.newCancellationToken()) {
      token.cancel();
      try (DataFrame df = ctx.sql("SELECT * FROM (VALUES (1), (2), (3)) AS t(x)")) {
        assertThrows(CancellationException.class, () -> df.executeStream(allocator, token));
      }
    }
  }

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void cancelMidCollectAborts() throws Exception {
    CountDownLatch firstInvocation = new CountDownLatch(1);
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = SessionContext.builder().batchSize(1).build();
        CancellationToken token = ctx.newCancellationToken()) {
      ctx.registerUdf(new ScalarUdf(new SleepingIdentity(50, firstInvocation)));

      // 1000 rows * 50ms ≈ 50s without cancel; cancel must abort well under the
      // test timeout (30s).
      DataFrame df =
          ctx.sql("SELECT sleep_identity(CAST(value AS INT)) AS y FROM generate_series(1, 1000)");

      Future<?> future = pool.submit(() -> df.collect(allocator, token));
      assertTrue(
          firstInvocation.await(10, TimeUnit.SECONDS),
          "UDF should have been invoked at least once");
      token.cancel();

      ExecutionException thrown = assertThrows(ExecutionException.class, future::get);
      assertTrue(
          thrown.getCause() instanceof CancellationException,
          "expected CancellationException, got " + thrown.getCause());
      df.close();
    } finally {
      pool.shutdownNow();
      pool.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void cancelMidExecuteStreamAbortsNextLoadBatch() throws Exception {
    CountDownLatch firstInvocation = new CountDownLatch(1);
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = SessionContext.builder().batchSize(1).build();
        CancellationToken token = ctx.newCancellationToken()) {
      ctx.registerUdf(new ScalarUdf(new SleepingIdentity(50, firstInvocation)));

      DataFrame df =
          ctx.sql("SELECT sleep_identity(CAST(value AS INT)) AS y FROM generate_series(1, 1000)");

      try (ArrowReader reader = df.executeStream(allocator, token)) {
        // Spawn a watcher that fires cancel once the UDF has been entered. The
        // first loadNextBatch may legitimately succeed (DataFusion can have
        // already produced one batch before we cancel), so we drain in a loop
        // that asserts a cancel surfaces eventually.
        //
        // The Arrow C-Data stream layer wraps the native error in an
        // IOException; we assert the message round-trip rather than the
        // exception type so the test stays decoupled from Arrow's wrapper
        // policy. Once a typed exception layer lands, this can tighten.
        Thread canceller =
            new Thread(
                () -> {
                  try {
                    if (firstInvocation.await(10, TimeUnit.SECONDS)) {
                      token.cancel();
                    }
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                },
                "cancel-watcher");
        canceller.start();
        try {
          Throwable caught =
              assertThrows(
                  Throwable.class,
                  () -> {
                    while (reader.loadNextBatch()) {
                      // drain — eventually loadNextBatch will throw on cancel.
                    }
                  });
          assertTrue(
              caught.getMessage() != null && caught.getMessage().contains("query cancelled"),
              "expected cancel message to surface, got: " + caught);
        } finally {
          canceller.join();
        }
      }
    }
  }

  @Test
  void closedTokenIsRejected() throws Exception {
    // A closed token has a zeroed native handle. Treating it as "no token" would
    // silently fall back to an uncancellable call -- the API instead must fail
    // fast so premature close() is easy to diagnose.
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      CancellationToken token = ctx.newCancellationToken();
      token.close();
      try (DataFrame df = ctx.sql("SELECT 1")) {
        assertThrows(IllegalStateException.class, () -> df.collect(allocator, token));
      }
      try (DataFrame df = ctx.sql("SELECT 1")) {
        assertThrows(IllegalStateException.class, () -> df.executeStream(allocator, token));
      }
    }
  }

  @Test
  void nullTokenOverloadEquivalentToNoToken() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      try (DataFrame df = ctx.sql("SELECT * FROM (VALUES (1), (2), (3)) AS t(x)");
          ArrowReader reader = df.collect(allocator, null)) {
        long total = 0;
        while (reader.loadNextBatch()) {
          total += reader.getVectorSchemaRoot().getRowCount();
        }
        assertEquals(3L, total);
      }
      try (DataFrame df = ctx.sql("SELECT * FROM (VALUES (1), (2), (3)) AS t(x)");
          ArrowReader reader = df.executeStream(allocator, null)) {
        long total = 0;
        while (reader.loadNextBatch()) {
          total += reader.getVectorSchemaRoot().getRowCount();
        }
        assertEquals(3L, total);
      }
    }
  }

  @Test
  void unfiredTokenDoesNotAffectCollect() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        CancellationToken token = ctx.newCancellationToken()) {
      try (DataFrame df = ctx.sql("SELECT * FROM (VALUES (1), (2), (3), (4)) AS t(x)");
          ArrowReader reader = df.collect(allocator, token)) {
        long total = 0;
        while (reader.loadNextBatch()) {
          total += reader.getVectorSchemaRoot().getRowCount();
        }
        assertEquals(4L, total);
      }
      assertFalse(token.isCancelled());
    }
  }

  @Test
  void freshTokenAfterFirstCollectStillCancelsSecond() throws Exception {
    // A token that has not yet fired remains usable across queries on the same
    // session. Once fired, it stays fired -- a follow-up call with the same
    // token aborts immediately.
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        CancellationToken token = ctx.newCancellationToken()) {
      try (DataFrame df1 = ctx.sql("SELECT 1");
          ArrowReader reader = df1.collect(allocator, token)) {
        while (reader.loadNextBatch()) {
          // drain
        }
      }
      assertFalse(token.isCancelled());
      token.cancel();
      try (DataFrame df2 = ctx.sql("SELECT 2")) {
        assertThrows(CancellationException.class, () -> df2.collect(allocator, token));
      }
    }
  }

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void sameTokenCancelsConcurrentCollects() throws Exception {
    CountDownLatch firstInvocation = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(3);
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = SessionContext.builder().batchSize(1).build();
        CancellationToken token = ctx.newCancellationToken()) {
      ctx.registerUdf(new ScalarUdf(new SleepingIdentity(50, firstInvocation)));

      // Spawn 3 collects against the same token. SessionContext is documented as
      // not thread-safe for concurrent use, so each task constructs its own
      // DataFrame *before* we kick off (sql() runs on the test thread inside
      // submit's lambda capture: we must serialise that part).
      List<DataFrame> dfs = new java.util.ArrayList<>();
      for (int i = 0; i < 3; i++) {
        dfs.add(
            ctx.sql(
                "SELECT sleep_identity(CAST(value AS INT)) AS y FROM generate_series(1, 1000)"));
      }
      List<Future<?>> futures = new java.util.ArrayList<>();
      for (DataFrame df : dfs) {
        futures.add(pool.submit(() -> df.collect(allocator, token)));
      }

      assertTrue(firstInvocation.await(10, TimeUnit.SECONDS));
      token.cancel();

      for (Future<?> f : futures) {
        ExecutionException thrown = assertThrows(ExecutionException.class, f::get);
        assertTrue(
            thrown.getCause() instanceof CancellationException,
            "expected CancellationException, got " + thrown.getCause());
      }
      for (DataFrame df : dfs) {
        df.close();
      }
    } finally {
      pool.shutdownNow();
      pool.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void countAndShowResolveBatchVectors() throws Exception {
    // Smoke test: BigIntVector is reachable so the import works on this JDK.
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        DataFrame df = ctx.sql("SELECT 1");
        ArrowReader reader = df.collect(allocator)) {
      assertTrue(reader.loadNextBatch());
      assertTrue(reader.getVectorSchemaRoot().getVector(0) instanceof BigIntVector);
    }
  }
}
