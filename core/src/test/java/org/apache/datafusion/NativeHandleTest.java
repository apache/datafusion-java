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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Unit tests for {@link NativeHandle}. These use fabricated handle values and never call into the
 * native library, so they exercise the lifetime state machine in isolation.
 */
class NativeHandleTest {

  private static final long HANDLE = 0xDEADBEEFL;

  private static NativeHandle newHandle() {
    return new NativeHandle(HANDLE, "test handle is closed");
  }

  @Test
  void rejectsZeroHandleAtConstruction() {
    assertThrows(IllegalArgumentException.class, () -> new NativeHandle(0, "unused"));
  }

  @Test
  void acquireReturnsHandleAndIsReentrantAcrossPins() {
    NativeHandle h = newHandle();
    assertEquals(HANDLE, h.acquire());
    assertEquals(HANDLE, h.acquire());
    h.release();
    h.release();
    assertEquals(HANDLE, h.claimQuietly());
  }

  @Test
  void acquireAfterClaimThrowsWithSuppliedMessage() {
    NativeHandle h = newHandle();
    assertEquals(HANDLE, h.claim());
    IllegalStateException e = assertThrows(IllegalStateException.class, h::acquire);
    assertEquals("test handle is closed", e.getMessage());
  }

  @Test
  void claimYieldsHandleExactlyOnce() {
    NativeHandle h = newHandle();
    assertEquals(HANDLE, h.claim());
    assertThrows(IllegalStateException.class, h::claim);
  }

  @Test
  void claimQuietlyReturnsZeroOnceClaimed() {
    NativeHandle h = newHandle();
    assertEquals(HANDLE, h.claimQuietly());
    assertEquals(0L, h.claimQuietly());
  }

  @Test
  void releaseWithoutAcquireIsRejected() {
    NativeHandle h = newHandle();
    assertThrows(IllegalStateException.class, h::release);
  }

  /**
   * The core guarantee: a claim (i.e. {@code close()} or a consuming operation) must not hand back
   * the raw handle for freeing while another thread is inside a JNI call that holds a pin.
   */
  @Test
  @Timeout(10)
  void claimBlocksUntilInFlightPinsDrain() throws Exception {
    NativeHandle h = newHandle();
    assertEquals(HANDLE, h.acquire());

    CountDownLatch claimStarted = new CountDownLatch(1);
    AtomicLong claimed = new AtomicLong(-1);
    Thread closer =
        new Thread(
            () -> {
              claimStarted.countDown();
              claimed.set(h.claim());
            });
    closer.start();

    assertTrue(claimStarted.await(5, TimeUnit.SECONDS));
    // Give the closer a chance to reach the drain wait, then confirm it is still parked.
    closer.join(200);
    assertTrue(closer.isAlive(), "claim() must not return while a pin is held");
    assertEquals(-1, claimed.get());

    // A late arrival is rejected immediately rather than queueing behind the claim.
    assertThrows(IllegalStateException.class, h::acquire);

    h.release();
    closer.join(5000);
    assertFalse(closer.isAlive());
    assertEquals(HANDLE, claimed.get());
  }

  /**
   * A claim in progress must survive interruption -- abandoning the drain would leak the native
   * allocation or, worse, free it under an in-flight call. The interrupt is deferred to the caller.
   */
  @Test
  @Timeout(10)
  void claimAbsorbsInterruptionAndRestoresTheFlag() throws Exception {
    NativeHandle h = newHandle();
    assertEquals(HANDLE, h.acquire());

    CountDownLatch started = new CountDownLatch(1);
    AtomicLong claimed = new AtomicLong(-1);
    AtomicReference<Boolean> interrupted = new AtomicReference<>();
    Thread closer =
        new Thread(
            () -> {
              started.countDown();
              claimed.set(h.claim());
              interrupted.set(Thread.currentThread().isInterrupted());
            });
    closer.start();

    assertTrue(started.await(5, TimeUnit.SECONDS));
    closer.join(200);
    closer.interrupt();

    closer.join(200);
    assertTrue(closer.isAlive(), "interrupt must not abandon the drain wait");

    h.release();
    closer.join(5000);
    assertFalse(closer.isAlive());
    assertEquals(HANDLE, claimed.get());
    assertTrue(interrupted.get(), "interrupt status must be restored for the caller");
  }

  /** Only one of many racing claimers may take the handle. */
  @Test
  @Timeout(30)
  void concurrentClaimsYieldExactlyOneWinner() throws Exception {
    for (int round = 0; round < 100; round++) {
      NativeHandle h = newHandle();
      int threads = 8;
      CountDownLatch start = new CountDownLatch(1);
      CountDownLatch done = new CountDownLatch(threads);
      AtomicLong winners = new AtomicLong();
      for (int i = 0; i < threads; i++) {
        new Thread(
                () -> {
                  try {
                    start.await();
                    if (h.claimQuietly() != 0) {
                      winners.incrementAndGet();
                    }
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  } finally {
                    done.countDown();
                  }
                })
            .start();
      }
      start.countDown();
      assertTrue(done.await(10, TimeUnit.SECONDS));
      assertEquals(1, winners.get());
    }
  }

  /**
   * Pins taken concurrently with a claim either succeed outright or fail; none observe a torn
   * state.
   */
  @Test
  @Timeout(30)
  void concurrentPinsNeverOutliveAClaim() throws Exception {
    for (int round = 0; round < 100; round++) {
      NativeHandle h = newHandle();
      int threads = 8;
      CountDownLatch start = new CountDownLatch(1);
      CountDownLatch done = new CountDownLatch(threads);
      for (int i = 0; i < threads; i++) {
        boolean claimer = i == 0;
        new Thread(
                () -> {
                  try {
                    start.await();
                    if (claimer) {
                      h.claimQuietly();
                    } else {
                      long raw = h.acquire();
                      try {
                        assertEquals(HANDLE, raw);
                      } finally {
                        h.release();
                      }
                    }
                  } catch (IllegalStateException expected) {
                    // Lost the race with the claim; that is the contract.
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  } finally {
                    done.countDown();
                  }
                })
            .start();
      }
      start.countDown();
      assertTrue(done.await(10, TimeUnit.SECONDS));
      // Whatever the interleaving, the handle is gone afterwards.
      assertEquals(0L, h.claimQuietly());
    }
  }
}
