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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Push-mode batch sink for a streaming table registered via {@link
 * SessionContext#registerStreamingTable(String, org.apache.arrow.vector.types.pojo.Schema, int)}.
 * Producers push {@link VectorSchemaRoot} batches; the registered table's consumer (DataFusion)
 * polls them out via the standard streaming-table plumbing.
 *
 * <p>Lifecycle:
 *
 * <ul>
 *   <li>{@link #write(VectorSchemaRoot)} -- send a batch. Blocks if the underlying mpsc channel is
 *       full (backpressure).
 *   <li>{@link #close()} -- end-of-stream signal; the consumer sees a clean EOF on its next poll.
 *       Idempotent; safe to call from a try-with-resources block.
 *   <li>{@link #fail(Throwable)} -- end-of-stream with an error. The consumer's next poll surfaces
 *       a {@link RuntimeException} with the supplied cause's message. Use this when the producer
 *       can't continue and the consumer should observe an error rather than a silent EOF.
 * </ul>
 *
 * <p><strong>Single-scan semantics.</strong> The streaming table this sink feeds can be queried at
 * most once. After the consumer side has drained, the sink stops being useful and the registered
 * table cannot be re-scanned. Callers who need re-scan should use the pull-mode {@link
 * SessionContext#registerTable(String, TableProvider)} path instead.
 *
 * <p><strong>Thread safety.</strong> Concurrent {@link #write(VectorSchemaRoot)} calls from
 * multiple producer threads are safe but their relative ordering is not specified; callers that
 * care about batch ordering should confine writes to a single thread. Concurrent {@link #close()}
 * (or {@link #fail(Throwable)}) racing with in-flight {@link #write(VectorSchemaRoot)} calls is
 * also safe: the close path drops the underlying mpsc sender first (which unblocks any write parked
 * on backpressure with an error), then waits for in-flight writes to drain before freeing the
 * native handle. New writes started after {@link #close()} began throw {@link
 * IllegalStateException} promptly without touching the native side.
 */
public final class TableSink implements AutoCloseable {
  static {
    NativeLibraryLoader.loadLibrary();
  }

  /**
   * Native handle to the {@code Box<Arc<TableSinkHandle>>}. Atomic: zeroed atomically by {@link
   * #close()} / {@link #fail(Throwable)} to forbid new writes; the lifecycle lock below makes the
   * eventual {@code dropHandleNative} wait for in-flight writes to drain.
   */
  private final AtomicLong nativeHandle;

  /**
   * Read-write lock guarding the native pointer's lifetime. {@link #write(VectorSchemaRoot)} holds
   * the read lock for the duration of the native call so a concurrent {@link #close()} cannot drop
   * the box while we're still using it. {@link #close()} / {@link #fail(Throwable)} take the write
   * lock around {@code dropHandleNative} so it waits for all in-flight writes to release.
   *
   * <p>Crucial detail: {@code closeSinkNative} (drops the producer-side {@code Sender}) runs
   * <strong>before</strong> we acquire the write lock. Otherwise a write parked on {@code
   * Sender::blocking_send} would never release the read lock and {@code close()} would deadlock.
   * Dropping the sender first lets the parked send return {@code Err} immediately, the write
   * returns, releases the read lock, and {@code close()} can acquire the write lock and drop the
   * box.
   */
  private final ReentrantReadWriteLock lifecycle = new ReentrantReadWriteLock();

  TableSink(long nativeHandle) {
    if (nativeHandle == 0) {
      throw new IllegalArgumentException("TableSink native handle is null");
    }
    this.nativeHandle = new AtomicLong(nativeHandle);
  }

  /**
   * Send a batch through the channel. Blocks if the channel is at capacity until the consumer
   * drains a batch or until the consumer side is dropped.
   *
   * <p>The batch's schema must match the schema this sink was registered with. The native side
   * exports the batch via Arrow's C Data Interface; the underlying buffers are reference-counted
   * and the caller can safely close {@code batch} immediately after this method returns.
   *
   * @throws IllegalArgumentException if {@code batch} is {@code null}.
   * @throws IllegalStateException if this sink has been closed.
   * @throws RuntimeException if the consumer side is gone (query cancelled or finished), if the
   *     batch's schema doesn't match the registered schema, or if the Arrow C Data export fails.
   */
  public void write(VectorSchemaRoot batch) {
    if (batch == null) {
      throw new IllegalArgumentException("write batch must be non-null");
    }
    // Hold the lifecycle read lock for the entire native call so a concurrent close() cannot
    // free the native box while we're parked inside Sender::blocking_send.
    lifecycle.readLock().lock();
    try {
      long h = nativeHandle.get();
      if (h == 0) {
        throw new IllegalStateException("TableSink is closed");
      }
      // The FFI scratch (ArrowArray + ArrowSchema) must share an allocator-root with the batch's
      // vectors so Data.exportVectorSchemaRoot can transfer ownership without crossing root
      // boundaries. We derive the allocator from the batch itself; an empty batch (no vectors)
      // has no allocator to borrow, but registerStreamingTable's schema requires at least one
      // column, so this path is unreachable in practice.
      BufferAllocator batchAllocator = batchAllocator(batch);
      try (ArrowArray ffiArray = ArrowArray.allocateNew(batchAllocator);
          ArrowSchema ffiSchema = ArrowSchema.allocateNew(batchAllocator)) {
        Data.exportVectorSchemaRoot(batchAllocator, batch, null, ffiArray, ffiSchema);
        writeBatchNative(h, ffiArray.memoryAddress(), ffiSchema.memoryAddress());
      }
    } finally {
      lifecycle.readLock().unlock();
    }
  }

  private static BufferAllocator batchAllocator(VectorSchemaRoot batch) {
    if (batch.getFieldVectors().isEmpty()) {
      throw new IllegalArgumentException(
          "TableSink.write requires a batch with at least one column");
    }
    FieldVector first = batch.getFieldVectors().get(0);
    BufferAllocator allocator = first.getAllocator();
    if (allocator == null) {
      throw new IllegalStateException(
          "VectorSchemaRoot's field vectors have no allocator; was the batch initialised?");
    }
    return allocator;
  }

  /**
   * End-of-stream signal: the consumer's next poll sees a clean EOF. Idempotent. Subsequent calls
   * to {@link #write(VectorSchemaRoot)} throw {@link IllegalStateException}.
   *
   * <p>Safe to call from {@code try-with-resources}. Note that {@code close()} alone signals a
   * <em>clean</em> end-of-stream; if the producer encountered an error, use {@link
   * #fail(Throwable)} instead so the consumer observes the error.
   */
  @Override
  public void close() {
    long h = nativeHandle.getAndSet(0);
    if (h == 0) {
      return; // already closed; idempotent.
    }
    // 1. Drop the producer-side Sender so any concurrent write parked on backpressure unblocks
    //    with an error and releases its read lock. Doing this BEFORE acquiring the write lock
    //    is what avoids the close-deadlocks-on-stuck-write race.
    closeSinkNative(h);
    // 2. Wait for in-flight writes to release the read lock, then drop the box. The Arc clones
    //    those writes hold keep the inner TableSinkHandle alive across this drop; the Box itself
    //    is what we're freeing.
    lifecycle.writeLock().lock();
    try {
      dropHandleNative(h);
    } finally {
      lifecycle.writeLock().unlock();
    }
  }

  /**
   * End-of-stream with error: the consumer's next poll surfaces a {@link RuntimeException} whose
   * message includes the supplied cause's message. After this call the sink is closed; further
   * {@link #write(VectorSchemaRoot)} or {@link #fail(Throwable)} calls throw.
   *
   * @throws IllegalArgumentException if {@code cause} is {@code null}.
   * @throws IllegalStateException if this sink has already been closed.
   */
  public void fail(Throwable cause) {
    if (cause == null) {
      throw new IllegalArgumentException("fail cause must be non-null");
    }
    long h = nativeHandle.getAndSet(0);
    if (h == 0) {
      throw new IllegalStateException("TableSink is closed");
    }
    String message = cause.getMessage();
    if (message == null) {
      message = cause.getClass().getName();
    }
    // Same two-phase shape as close(): record the terminal error + drop the sender first so
    // any parked write unblocks; then take the write lock and drop the box.
    failSinkNative(h, message);
    lifecycle.writeLock().lock();
    try {
      dropHandleNative(h);
    } finally {
      lifecycle.writeLock().unlock();
    }
  }

  private static native void writeBatchNative(long handle, long arrayAddr, long schemaAddr);

  private static native void closeSinkNative(long handle);

  private static native void failSinkNative(long handle, String message);

  private static native void dropHandleNative(long handle);
}
