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

import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A handle that signals an in-flight {@link
 * DataFrame#collect(org.apache.arrow.memory.BufferAllocator, CancellationToken)} or {@link
 * DataFrame#executeStream(org.apache.arrow.memory.BufferAllocator, CancellationToken)} to abort.
 *
 * <p>Allocate via {@link SessionContext#newCancellationToken()}, pass to the desired call from one
 * thread, and invoke {@link #cancel()} from another. The running query aborts at its next
 * cooperative poll point. The exception type depends on the call site: {@code collect(..., token)}
 * and pre-stream cancellation in {@code executeStream(..., token)} surface {@link
 * CancellationException}; mid-stream cancellation in {@code executeStream} surfaces from {@link
 * org.apache.arrow.vector.ipc.ArrowReader#loadNextBatch} as a {@link java.io.IOException} whose
 * message contains {@code "query cancelled"} (the Arrow C-data wrapper hides the typed signal). See
 * the {@code executeStream} Javadoc for the full contract.
 *
 * <p>The token is not bound to any particular DataFrame; the same token may be passed to several
 * concurrent {@code collect} / {@code executeStream} calls, and {@link #cancel()} fires all of them
 * at once. Once cancelled, {@link #isCancelled()} returns {@code true} permanently — to "reset",
 * allocate a fresh token. This matches the underlying {@code tokio_util::sync::CancellationToken}
 * contract.
 *
 * <p>Instances are safe to call {@link #cancel()} / {@link #isCancelled()} on from any thread, and
 * must be {@link #close() closed} to release the native handle. {@code close()} is idempotent and a
 * no-op once invoked.
 */
public final class CancellationToken implements AutoCloseable {
  static {
    NativeLibraryLoader.loadLibrary();
  }

  // Atomic so concurrent close + cancel/isCancelled/handle-pass-to-JNI cannot
  // double-free or pass a stale handle to the native side. Reads observe either
  // a live registry id or the post-close 0 sentinel; the close path uses
  // getAndSet so only one thread can issue closeToken.
  private final AtomicLong nativeHandle;

  CancellationToken(long nativeHandle) {
    if (nativeHandle == 0) {
      throw new IllegalArgumentException("CancellationToken native handle is null");
    }
    this.nativeHandle = new AtomicLong(nativeHandle);
  }

  /**
   * The internal native handle, or {@code 0} if this token is closed. Package-private so {@link
   * DataFrame} can pass it across JNI. The native side's registry lookup gracefully rejects a
   * closed handle, so a race between {@code handle()} and {@link #close()} is bounded to a clean
   * "closed" error rather than a use-after-free.
   */
  long handle() {
    return nativeHandle.get();
  }

  /**
   * Signal the token. Any {@code collect} or {@code executeStream} call that received this token
   * aborts at its next poll point. The thrown exception type depends on whether the cancel reaches
   * the call before or after the JNI call returns — see the class-level Javadoc. Idempotent:
   * subsequent calls are no-ops.
   *
   * @throws IllegalStateException if this token is closed.
   */
  public void cancel() {
    long h = nativeHandle.get();
    if (h == 0) {
      throw new IllegalStateException("CancellationToken is closed");
    }
    cancelToken(h);
  }

  /**
   * @return {@code true} if {@link #cancel()} has been invoked on this token, {@code false}
   *     otherwise. Non-blocking.
   * @throws IllegalStateException if this token is closed.
   */
  public boolean isCancelled() {
    long h = nativeHandle.get();
    if (h == 0) {
      throw new IllegalStateException("CancellationToken is closed");
    }
    return isCancelledToken(h);
  }

  /**
   * Release the native handle. Idempotent. After {@code close()}, {@link #cancel()} and {@link
   * #isCancelled()} throw {@link IllegalStateException}. Closing a token that already fired is
   * harmless and does not cancel anything else; queries that already received the cancel signal
   * remain aborted.
   */
  @Override
  public void close() {
    long h = nativeHandle.getAndSet(0L);
    if (h != 0) {
      closeToken(h);
    }
  }

  private static native void cancelToken(long handle);

  private static native boolean isCancelledToken(long handle);

  private static native void closeToken(long handle);
}
