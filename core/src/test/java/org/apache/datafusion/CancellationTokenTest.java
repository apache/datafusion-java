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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class CancellationTokenTest {

  @Test
  void freshTokenIsNotCancelled() {
    try (SessionContext ctx = new SessionContext();
        CancellationToken token = ctx.newCancellationToken()) {
      assertFalse(token.isCancelled());
    }
  }

  @Test
  void cancelMakesIsCancelledTrue() {
    try (SessionContext ctx = new SessionContext();
        CancellationToken token = ctx.newCancellationToken()) {
      token.cancel();
      assertTrue(token.isCancelled());
    }
  }

  @Test
  void cancelIsIdempotent() {
    try (SessionContext ctx = new SessionContext();
        CancellationToken token = ctx.newCancellationToken()) {
      token.cancel();
      token.cancel();
      token.cancel();
      assertTrue(token.isCancelled());
    }
  }

  @Test
  void closeIsIdempotent() {
    try (SessionContext ctx = new SessionContext()) {
      CancellationToken token = ctx.newCancellationToken();
      token.close();
      token.close();
    }
  }

  @Test
  void operationsAfterCloseThrow() {
    try (SessionContext ctx = new SessionContext()) {
      CancellationToken token = ctx.newCancellationToken();
      token.close();
      assertThrows(IllegalStateException.class, token::cancel);
      assertThrows(IllegalStateException.class, token::isCancelled);
    }
  }

  @Test
  void closeAfterCancelDoesNotThrow() {
    try (SessionContext ctx = new SessionContext()) {
      CancellationToken token = ctx.newCancellationToken();
      token.cancel();
      token.close();
    }
  }

  @Test
  void tokensFromSameSessionAreIndependent() {
    try (SessionContext ctx = new SessionContext();
        CancellationToken a = ctx.newCancellationToken();
        CancellationToken b = ctx.newCancellationToken()) {
      a.cancel();
      assertTrue(a.isCancelled());
      assertFalse(b.isCancelled());
    }
  }

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void closeRacingWithCancelDoesNotCrashJvm() throws Exception {
    // Stress the close()/cancel()/isCancelled() race that a per-token AtomicLong
    // + native registry are designed to make safe. Without the atomic+registry
    // pair, a thread could read a stale native pointer from a token that
    // another thread has already closed and call into freed memory.
    final int iterations = 200;
    final int callers = 4;
    ExecutorService pool = Executors.newCachedThreadPool();
    AtomicInteger illegalState = new AtomicInteger();
    AtomicInteger ok = new AtomicInteger();
    try (SessionContext ctx = new SessionContext()) {
      for (int i = 0; i < iterations; i++) {
        final CancellationToken token = ctx.newCancellationToken();
        CountDownLatch start = new CountDownLatch(1);
        Runnable cancelOrCheck =
            () -> {
              try {
                start.await();
                token.cancel();
                token.isCancelled();
                ok.incrementAndGet();
              } catch (IllegalStateException e) {
                illegalState.incrementAndGet();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            };
        Runnable closer =
            () -> {
              try {
                start.await();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
              }
              token.close();
            };
        for (int c = 0; c < callers; c++) {
          pool.submit(cancelOrCheck);
        }
        pool.submit(closer);
        start.countDown();
      }
    } finally {
      pool.shutdown();
      assertTrue(pool.awaitTermination(20, TimeUnit.SECONDS));
    }
    // Survival is the actual assertion: no JVM crash, no native panic. Either
    // the cancel/isCancelled won the race (ok) or close did (IllegalStateException).
    assertTrue(ok.get() + illegalState.get() > 0);
  }

  @Test
  void tokenOutlivesItsSession() {
    CancellationToken token;
    try (SessionContext ctx = new SessionContext()) {
      token = ctx.newCancellationToken();
    }
    // Closing the session does not invalidate the token; cancel() and
    // isCancelled() must continue to work, and close() must not panic.
    assertFalse(token.isCancelled());
    token.cancel();
    assertTrue(token.isCancelled());
    token.close();
  }
}
