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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Concurrency contract for {@link DataFrame}: {@link DataFrame#close()} and the consuming
 * operations must not release the native plan while another thread is inside a JNI call on it. See
 * issue #40.
 */
class DataFrameConcurrencyTest {

  /** Exactly one of several threads racing to consume a DataFrame may win. */
  @Test
  @Timeout(120)
  void concurrentCollectYieldsExactlyOneWinner() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      for (int round = 0; round < 20; round++) {
        DataFrame df = ctx.sql("select 1 as a");
        int threads = 6;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger winners = new AtomicInteger();
        List<Throwable> unexpected = Collections.synchronizedList(new ArrayList<>());
        List<ArrowReader> readers = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < threads; i++) {
          new Thread(
                  () -> {
                    try {
                      start.await();
                      readers.add(df.collect(allocator));
                      winners.incrementAndGet();
                    } catch (IllegalStateException lost) {
                      // Another thread consumed the DataFrame first; that is the contract.
                    } catch (Throwable t) {
                      unexpected.add(t);
                    } finally {
                      done.countDown();
                    }
                  },
                  "collector-" + i)
              .start();
        }

        start.countDown();
        assertTrue(done.await(60, TimeUnit.SECONDS));
        assertTrue(unexpected.isEmpty(), "unexpected failures: " + unexpected);
        assertEquals(1, winners.get(), "a DataFrame must be consumable exactly once");
        for (ArrowReader reader : readers) {
          reader.close();
        }
        df.close();
      }
    }
  }

  /**
   * {@code close()} must block until an in-flight execution returns. A {@link TableProvider} whose
   * {@code scan} parks on a latch holds the native call open for as long as the test needs.
   */
  @Test
  @Timeout(120)
  void closeWaitsForAnInFlightExecution() throws Exception {
    try (SessionContext ctx = new SessionContext()) {
      LatchedTableProvider provider = new LatchedTableProvider();
      ctx.registerTable("t", provider);
      DataFrame df = ctx.sql("select * from t");

      Thread counter = new Thread(df::count, "counter");
      counter.start();
      assertTrue(provider.insideScan.await(60, TimeUnit.SECONDS), "scan() was never reached");

      Thread closer = new Thread(df::close, "closer");
      closer.start();
      closer.join(500);
      assertTrue(closer.isAlive(), "close() must not free the plan while a call is in flight");

      provider.releaseScan.countDown();
      counter.join(60_000);
      closer.join(60_000);
      assertFalse(counter.isAlive());
      assertFalse(closer.isAlive());

      assertThrows(IllegalStateException.class, df::count);
    }
  }

  /** Non-consuming operations are shared, not serialised: concurrent readers all succeed. */
  @Test
  @Timeout(120)
  void concurrentNonConsumingOperationsAllSucceed() throws Exception {
    try (SessionContext ctx = new SessionContext();
        DataFrame df = ctx.sql("select 1 as a")) {
      int threads = 6;
      CountDownLatch start = new CountDownLatch(1);
      CountDownLatch done = new CountDownLatch(threads);
      List<Throwable> unexpected = Collections.synchronizedList(new ArrayList<>());

      for (int i = 0; i < threads; i++) {
        new Thread(
                () -> {
                  try {
                    start.await();
                    for (int j = 0; j < 20; j++) {
                      assertEquals(1, df.count());
                      assertEquals(1, df.schema().getFields().size());
                    }
                  } catch (Throwable t) {
                    unexpected.add(t);
                  } finally {
                    done.countDown();
                  }
                },
                "reader-" + i)
            .start();
      }

      start.countDown();
      assertTrue(done.await(60, TimeUnit.SECONDS));
      assertTrue(unexpected.isEmpty(), "unexpected failures: " + unexpected);
    }
  }

  /**
   * The two-handle set operations pin both DataFrames at once. Pins never block, so opposing pin
   * orders across threads cannot deadlock -- this test hangs (and times out) if that ever changes.
   */
  @Test
  @Timeout(120)
  void opposingSetOperationsDoNotDeadlock() throws Exception {
    try (SessionContext ctx = new SessionContext();
        DataFrame left = ctx.sql("select 1 as a");
        DataFrame right = ctx.sql("select 2 as a")) {
      int threads = 6;
      CountDownLatch start = new CountDownLatch(1);
      CountDownLatch done = new CountDownLatch(threads);
      List<Throwable> unexpected = Collections.synchronizedList(new ArrayList<>());

      for (int i = 0; i < threads; i++) {
        boolean forward = i % 2 == 0;
        new Thread(
                () -> {
                  try {
                    start.await();
                    for (int j = 0; j < 20; j++) {
                      try (DataFrame u = forward ? left.union(right) : right.union(left)) {
                        assertEquals(2, u.count());
                      }
                    }
                  } catch (Throwable t) {
                    unexpected.add(t);
                  } finally {
                    done.countDown();
                  }
                },
                "unioner-" + i)
            .start();
      }

      start.countDown();
      assertTrue(done.await(60, TimeUnit.SECONDS), "set operations deadlocked");
      assertTrue(unexpected.isEmpty(), "unexpected failures: " + unexpected);
    }
  }

  /** A {@link TableProvider} whose scan parks until the test releases it. */
  private static final class LatchedTableProvider implements TableProvider {
    private final Schema schema =
        new Schema(Collections.singletonList(Field.nullable("a", new ArrowType.Int(32, true))));
    private final byte[] emptyStream = emptyIpcStream(schema);
    private final CountDownLatch insideScan = new CountDownLatch(1);
    private final CountDownLatch releaseScan = new CountDownLatch(1);

    @Override
    public Schema schema() {
      return schema;
    }

    @Override
    public ArrowReader scan(BufferAllocator allocator) {
      insideScan.countDown();
      boolean interrupted = false;
      while (true) {
        try {
          releaseScan.await();
          break;
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
      return new ArrowStreamReader(new ByteArrayInputStream(emptyStream), allocator);
    }

    /** An Arrow IPC stream carrying {@code schema} and zero batches. */
    private static byte[] emptyIpcStream(Schema schema) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (BufferAllocator tmp = new RootAllocator();
          VectorSchemaRoot root = VectorSchemaRoot.create(schema, tmp);
          ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(baos))) {
        writer.start();
        writer.end();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      return baos.toByteArray();
    }
  }
}
