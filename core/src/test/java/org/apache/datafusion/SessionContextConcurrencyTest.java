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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Concurrency contract for {@link SessionContext}: a {@link SessionContext#close()} racing with
 * work on other threads must never free the native session out from under an in-flight JNI call.
 * See issue #40.
 */
class SessionContextConcurrencyTest {

  @Test
  void useAfterCloseThrowsIllegalState() {
    SessionContext ctx = new SessionContext();
    ctx.close();
    assertThrows(IllegalStateException.class, () -> ctx.sql("select 1"));
    assertThrows(IllegalStateException.class, () -> ctx.tableExists("t"));
    assertThrows(IllegalStateException.class, ctx::memoryUsage);
  }

  /**
   * The central guarantee. {@code registerTable} calls {@link TableProvider#schema()} on the
   * calling thread while the native handle is pinned, which gives us a deterministic way to hold a
   * call open across a concurrent {@code close()}.
   */
  @Test
  @Timeout(60)
  void closeWaitsForAnInFlightCall() throws Exception {
    SessionContext ctx = new SessionContext();
    CountDownLatch insideSchema = new CountDownLatch(1);
    CountDownLatch releaseSchema = new CountDownLatch(1);

    TableProvider blocking =
        new TableProvider() {
          @Override
          public Schema schema() {
            insideSchema.countDown();
            awaitUninterruptibly(releaseSchema);
            return new Schema(
                Collections.singletonList(Field.nullable("a", new ArrowType.Int(32, true))));
          }

          @Override
          public ArrowReader scan(BufferAllocator allocator) {
            throw new UnsupportedOperationException("not scanned by this test");
          }
        };

    Thread registrar = new Thread(() -> ctx.registerTable("t", blocking), "registrar");
    registrar.start();
    assertTrue(insideSchema.await(30, TimeUnit.SECONDS), "registerTable never reached schema()");

    Thread closer = new Thread(ctx::close, "closer");
    closer.start();
    closer.join(500);
    assertTrue(closer.isAlive(), "close() must not free the session while a call is in flight");

    releaseSchema.countDown();
    registrar.join(30_000);
    closer.join(30_000);
    assertFalse(registrar.isAlive());
    assertFalse(closer.isAlive());

    assertThrows(IllegalStateException.class, () -> ctx.sql("select 1"));
  }

  /**
   * Hammer a context from several threads while closing it. Before the fix this raced on a plain
   * {@code long} field and could dereference a freed {@code SessionContext}; the only failure
   * permitted now is {@link IllegalStateException} from losing the race.
   */
  @Test
  @Timeout(300)
  void concurrentQueriesRacingCloseOnlyEverFailWithIllegalState() throws Exception {
    for (int round = 0; round < 5; round++) {
      SessionContext ctx = new SessionContext();
      int threads = 6;
      CountDownLatch start = new CountDownLatch(1);
      CountDownLatch done = new CountDownLatch(threads);
      CountDownLatch firstSuccess = new CountDownLatch(1);
      List<Throwable> unexpected = Collections.synchronizedList(new ArrayList<>());

      for (int i = 0; i < threads; i++) {
        new Thread(
                () -> {
                  try {
                    start.await();
                    for (int j = 0; j < 20; j++) {
                      try (DataFrame df = ctx.sql("select 1")) {
                        df.count();
                        firstSuccess.countDown();
                      } catch (IllegalStateException closedUnderUs) {
                        return;
                      }
                    }
                  } catch (Throwable t) {
                    unexpected.add(t);
                  } finally {
                    done.countDown();
                  }
                },
                "querier-" + i)
            .start();
      }

      start.countDown();
      // Let real work overlap the close rather than closing an idle context.
      firstSuccess.await(30, TimeUnit.SECONDS);
      ctx.close();

      assertTrue(done.await(120, TimeUnit.SECONDS), "workers did not finish");
      assertTrue(unexpected.isEmpty(), "unexpected failures: " + unexpected);
    }
  }

  /** {@code close()} is idempotent even when several threads call it at once. */
  @Test
  @Timeout(60)
  void concurrentCloseIsIdempotent() throws Exception {
    SessionContext ctx = new SessionContext();
    int threads = 8;
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threads);
    List<Throwable> unexpected = Collections.synchronizedList(new ArrayList<>());

    for (int i = 0; i < threads; i++) {
      new Thread(
              () -> {
                try {
                  start.await();
                  ctx.close();
                } catch (Throwable t) {
                  unexpected.add(t);
                } finally {
                  done.countDown();
                }
              },
              "closer-" + i)
          .start();
    }

    start.countDown();
    assertTrue(done.await(30, TimeUnit.SECONDS));
    assertTrue(unexpected.isEmpty(), "close() must be idempotent, saw: " + unexpected);
  }

  private static void awaitUninterruptibly(CountDownLatch latch) {
    boolean interrupted = false;
    while (true) {
      try {
        latch.await();
        break;
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }
}
