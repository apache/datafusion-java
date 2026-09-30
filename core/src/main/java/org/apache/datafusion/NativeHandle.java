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

/**
 * Owns a pointer to a native allocation and serialises its lifetime against concurrent use.
 *
 * <p>A bare {@code long} field guarded by {@code if (handle == 0) throw} is a time-of-check /
 * time-of-use bug: a thread can read a live handle, and a {@code close()} on another thread can
 * free the allocation before the first thread's JNI call dereferences it. This class closes that
 * window by pinning the handle for the duration of each native call and deferring the free until
 * every in-flight call has drained.
 *
 * <p>Usage is a pin around each native call:
 *
 * <pre>{@code
 * long h = handle.acquire();
 * try {
 *   someNativeCall(h, ...);
 * } finally {
 *   handle.release();
 * }
 * }</pre>
 *
 * <p>and a claim for the operation that hands the pointer back to Rust to be freed or consumed:
 *
 * <pre>{@code
 * long h = handle.claimQuietly();
 * if (h != 0) {
 *   closeNative(h);
 * }
 * }</pre>
 *
 * <p>Two properties matter for callers:
 *
 * <ul>
 *   <li><b>{@link #acquire()} never blocks.</b> It either pins immediately or throws. Callers may
 *       therefore hold a pin on one handle while pinning a second -- as the {@code DataFrame} set
 *       operations and joins do -- without any risk of deadlock. A read/write lock would not permit
 *       this: its readers queue behind a waiting writer, so two threads pinning the same pair in
 *       opposite orders with closes interleaved could deadlock.
 *   <li><b>{@link #claim()} is the only blocking operation,</b> and it always makes progress: a
 *       thread that holds a pin is inside a native call and never claims, so the pins it is waiting
 *       on are guaranteed to be released.
 * </ul>
 *
 * <p>The monitor is held only for bookkeeping, never across a native call, so independent
 * operations on the same object still run concurrently.
 */
final class NativeHandle {

  /** Message for the {@link IllegalStateException} raised once the handle is gone. */
  private final String closedMessage;

  /** The native pointer, or 0 once claimed. */
  private long handle;

  /** Number of threads currently inside a native call with this handle pinned. */
  private int inFlight;

  /**
   * Set once a claim has begun. New pins are refused from this point on, even though {@link
   * #handle} stays readable until the drain completes.
   */
  private boolean claimed;

  /**
   * @param handle a non-zero native pointer
   * @param closedMessage the {@link IllegalStateException} message to use once the handle is gone;
   *     lets each owner keep its own wording
   */
  NativeHandle(long handle, String closedMessage) {
    if (handle == 0) {
      throw new IllegalArgumentException(closedMessage + ": native handle is null");
    }
    this.handle = handle;
    this.closedMessage = closedMessage;
  }

  /**
   * Pin the handle for the duration of one native call and return it. Never blocks.
   *
   * @throws IllegalStateException if the handle has been claimed
   */
  synchronized long acquire() {
    if (claimed) {
      throw new IllegalStateException(closedMessage);
    }
    inFlight++;
    return handle;
  }

  /**
   * Release a pin taken by {@link #acquire()}. Must be called from a {@code finally} block so that
   * a native call which throws still drains.
   */
  synchronized void release() {
    if (inFlight == 0) {
      throw new IllegalStateException("release() without a matching acquire()");
    }
    inFlight--;
    if (inFlight == 0 && claimed) {
      notifyAll();
    }
  }

  /**
   * Take exclusive ownership of the handle, blocking until in-flight calls drain, and return it for
   * freeing or consumption. Subsequent {@link #acquire()} calls throw.
   *
   * @throws IllegalStateException if the handle has already been claimed
   */
  synchronized long claim() {
    if (claimed) {
      throw new IllegalStateException(closedMessage);
    }
    return drainAndTake();
  }

  /**
   * As {@link #claim()}, but returns 0 rather than throwing when the handle has already been
   * claimed. Used by {@code close()}, which is specified to be idempotent.
   */
  synchronized long claimQuietly() {
    if (claimed) {
      return 0;
    }
    return drainAndTake();
  }

  /**
   * Refuse further pins, wait for the outstanding ones to drain, then surrender the pointer.
   *
   * <p>Interruption is deferred rather than obeyed: returning early would either leak the native
   * allocation or free it under a call that is still using it. The flag is restored so the caller
   * can act on it after the handle is safely accounted for.
   */
  private long drainAndTake() {
    claimed = true;
    boolean interrupted = false;
    while (inFlight > 0) {
      try {
        wait();
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }
    long claimedHandle = handle;
    handle = 0;
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    return claimedHandle;
  }
}
