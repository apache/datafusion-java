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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

class SessionContextStreamingTableTest {

  /** Build a `(id: int32 not-null)` schema. */
  private static Schema oneIntSchema() {
    return new Schema(
        Collections.singletonList(
            new Field("id", FieldType.notNullable(new ArrowType.Int(32, true)), null)));
  }

  /**
   * Build a single-column int batch with the given values, allocated against the supplied root
   * allocator. Caller closes the returned root.
   */
  private static VectorSchemaRoot makeIntBatch(BufferAllocator allocator, int[] values) {
    VectorSchemaRoot root = VectorSchemaRoot.create(oneIntSchema(), allocator);
    IntVector vec = (IntVector) root.getVector(0);
    vec.allocateNew(values.length);
    for (int i = 0; i < values.length; i++) {
      vec.set(i, values[i]);
    }
    vec.setValueCount(values.length);
    root.setRowCount(values.length);
    return root;
  }

  @Test
  void writeAndScanFromSameThread() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      try (TableSink sink = ctx.registerStreamingTable("t", oneIntSchema(), 4)) {
        try (VectorSchemaRoot b1 = makeIntBatch(allocator, new int[] {1, 2, 3});
            VectorSchemaRoot b2 = makeIntBatch(allocator, new int[] {4, 5})) {
          sink.write(b1);
          sink.write(b2);
        }
      } // close() = EOF before the SQL query runs.
      try (DataFrame df = ctx.sql("SELECT count(*) FROM t");
          ArrowReader reader = df.collect(allocator)) {
        assertTrue(reader.loadNextBatch());
        BigIntVector count = (BigIntVector) reader.getVectorSchemaRoot().getVector(0);
        assertEquals(5L, count.get(0));
      }
    }
  }

  @Test
  void producerOnSeparateThread() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      TableSink sink = ctx.registerStreamingTable("t", oneIntSchema(), 2);
      Thread producer =
          new Thread(
              () -> {
                try (VectorSchemaRoot b1 = makeIntBatch(allocator, new int[] {10, 20, 30});
                    VectorSchemaRoot b2 = makeIntBatch(allocator, new int[] {40})) {
                  sink.write(b1);
                  sink.write(b2);
                } finally {
                  sink.close();
                }
              });
      producer.start();
      try (DataFrame df = ctx.sql("SELECT sum(id) FROM t");
          ArrowReader reader = df.collect(allocator)) {
        assertTrue(reader.loadNextBatch());
        BigIntVector sum = (BigIntVector) reader.getVectorSchemaRoot().getVector(0);
        assertEquals(100L, sum.get(0));
      }
      producer.join(5_000);
      assertTrue(!producer.isAlive(), "producer thread did not finish");
    }
  }

  @Test
  void closeSignalsEndOfStream() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      try (TableSink sink = ctx.registerStreamingTable("t", oneIntSchema(), 4)) {
        try (VectorSchemaRoot b = makeIntBatch(allocator, new int[] {1, 2})) {
          sink.write(b);
        }
        sink.close();
      }
      try (DataFrame df = ctx.sql("SELECT count(*) FROM t");
          ArrowReader reader = df.collect(allocator)) {
        assertTrue(reader.loadNextBatch());
        BigIntVector count = (BigIntVector) reader.getVectorSchemaRoot().getVector(0);
        assertEquals(2L, count.get(0));
      }
    }
  }

  @Test
  void failPropagatesError() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      TableSink sink = ctx.registerStreamingTable("t", oneIntSchema(), 4);
      try (VectorSchemaRoot b = makeIntBatch(allocator, new int[] {1, 2})) {
        sink.write(b);
      }
      sink.fail(new RuntimeException("producer-side boom"));
      RuntimeException e =
          assertThrows(
              RuntimeException.class,
              () -> {
                try (DataFrame df = ctx.sql("SELECT count(*) FROM t");
                    ArrowReader reader = df.collect(allocator)) {
                  // collect() may either throw directly or surface on first loadNextBatch.
                  while (reader.loadNextBatch()) {
                    // drain
                  }
                }
              });
      assertTrue(
          e.getMessage().contains("producer-side boom"),
          () -> "unexpected error message: " + e.getMessage());
    }
  }

  /**
   * Regression test for the close()-vs-blocked-write() use-after-free. With capacity 1 and one
   * successful write, the data channel is full; a second write parks inside {@code
   * Sender::blocking_send}. We then close() from another thread. close() must:
   *
   * <ol>
   *   <li>Drop the sender so the parked write returns Err and releases its read lock.
   *   <li>Wait for the read lock before freeing the native box.
   * </ol>
   *
   * <p>If either step is missing, dropHandleNative would free the box while writeBatchNative still
   * holds a borrowed pointer -- use-after-free.
   */
  @Test
  void closeDuringBlockedWriteIsSafe() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      TableSink sink = ctx.registerStreamingTable("t", oneIntSchema(), 1);
      AtomicReference<Throwable> producerError = new AtomicReference<>();
      CountDownLatch firstWriteDone = new CountDownLatch(1);
      Thread producer =
          new Thread(
              () -> {
                try (VectorSchemaRoot b1 = makeIntBatch(allocator, new int[] {1});
                    VectorSchemaRoot b2 = makeIntBatch(allocator, new int[] {2})) {
                  sink.write(b1); // fills the capacity-1 channel
                  firstWriteDone.countDown();
                  sink.write(b2); // parks on blocking_send; close() unblocks with Err
                } catch (Throwable t) {
                  producerError.set(t);
                }
              });
      producer.start();
      assertTrue(firstWriteDone.await(5, TimeUnit.SECONDS));
      // Give the producer a moment to actually park on the second write.
      Thread.sleep(100);
      // close() from this (main) thread races with the producer's parked write. Must not UAF.
      sink.close();
      producer.join(5_000);
      assertTrue(!producer.isAlive(), "producer thread did not unblock after close()");
      // The producer's second write should have surfaced the consumer-closed error.
      assertNotNull(
          producerError.get(),
          "producer's parked write should have thrown when close() dropped the receiver");
      assertTrue(
          producerError.get() instanceof RuntimeException,
          () -> "expected RuntimeException, got " + producerError.get());
    }
  }

  /**
   * Regression test for the "fail() blocks on full channel pre-query" deadlock. With capacity 1 and
   * one successful write, the data channel is full. The producer then calls fail() before any
   * consumer starts -- if fail() were implemented as `tx.blocking_send(Err(...))`, it would park
   * forever because no consumer is draining. The sideband-error implementation must terminate
   * synchronously and the consumer must observe the error when it eventually runs.
   */
  @Test
  void failOnFullChannelBeforeConsumer() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      TableSink sink = ctx.registerStreamingTable("t", oneIntSchema(), 1);
      try (VectorSchemaRoot b = makeIntBatch(allocator, new int[] {1})) {
        sink.write(b); // channel now full
      }
      // Run fail() on a separate thread with a hard timeout so a regression doesn't hang the
      // whole test suite. The fix must let fail() return promptly even when the channel is full.
      Thread failer = new Thread(() -> sink.fail(new RuntimeException("pre-consumer boom")));
      failer.start();
      failer.join(5_000);
      assertTrue(
          !failer.isAlive(),
          "fail() blocked on a full channel; sideband-error path is missing or broken");
      // Consumer now runs; observes the queued batch first, then the terminal error.
      RuntimeException e =
          assertThrows(
              RuntimeException.class,
              () -> {
                try (DataFrame df = ctx.sql("SELECT count(*) FROM t");
                    ArrowReader reader = df.collect(allocator)) {
                  while (reader.loadNextBatch()) {
                    // drain
                  }
                }
              });
      assertTrue(
          e.getMessage().contains("pre-consumer boom"),
          () -> "unexpected error message: " + e.getMessage());
    }
  }

  @Test
  void backpressureBlocksProducer() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      // Capacity 1: first write goes through; second blocks until the consumer drains.
      TableSink sink = ctx.registerStreamingTable("t", oneIntSchema(), 1);
      CountDownLatch firstWriteDone = new CountDownLatch(1);
      CountDownLatch secondWriteStarted = new CountDownLatch(1);
      AtomicReference<Throwable> producerError = new AtomicReference<>();
      Thread producer =
          new Thread(
              () -> {
                try (VectorSchemaRoot b1 = makeIntBatch(allocator, new int[] {1});
                    VectorSchemaRoot b2 = makeIntBatch(allocator, new int[] {2});
                    VectorSchemaRoot b3 = makeIntBatch(allocator, new int[] {3})) {
                  sink.write(b1);
                  firstWriteDone.countDown();
                  // Two more writes; one of these must block because the channel is at capacity.
                  secondWriteStarted.countDown();
                  sink.write(b2);
                  sink.write(b3);
                } catch (Throwable t) {
                  producerError.set(t);
                } finally {
                  sink.close();
                }
              });
      producer.start();
      assertTrue(firstWriteDone.await(5, TimeUnit.SECONDS));
      assertTrue(secondWriteStarted.await(5, TimeUnit.SECONDS));
      // The producer must still be alive at this point because it's blocked on the second
      // (or third) write. No way to assert "thread is parked" cleanly from JUnit, so rely on
      // the consumer side draining and observing all three batches.
      try (DataFrame df = ctx.sql("SELECT count(*) FROM t");
          ArrowReader reader = df.collect(allocator)) {
        assertTrue(reader.loadNextBatch());
        BigIntVector count = (BigIntVector) reader.getVectorSchemaRoot().getVector(0);
        assertEquals(3L, count.get(0));
      }
      producer.join(5_000);
      assertTrue(!producer.isAlive(), "producer thread did not finish");
      assertEquals(null, producerError.get(), () -> "producer error: " + producerError.get());
    }
  }

  @Test
  void secondScanThrows() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      try (TableSink sink = ctx.registerStreamingTable("t", oneIntSchema(), 4)) {
        try (VectorSchemaRoot b = makeIntBatch(allocator, new int[] {7})) {
          sink.write(b);
        }
      }
      try (DataFrame df = ctx.sql("SELECT count(*) FROM t");
          ArrowReader reader = df.collect(allocator)) {
        assertTrue(reader.loadNextBatch());
      }
      // Second scan against the same registered streaming table must fail.
      RuntimeException e =
          assertThrows(
              RuntimeException.class,
              () -> {
                try (DataFrame df2 = ctx.sql("SELECT count(*) FROM t");
                    ArrowReader r2 = df2.collect(allocator)) {
                  while (r2.loadNextBatch()) {
                    // drain
                  }
                }
              });
      assertTrue(
          e.getMessage().toLowerCase().contains("single-scan")
              || e.getMessage().toLowerCase().contains("already consumed"),
          () -> "unexpected error message: " + e.getMessage());
    }
  }

  @Test
  void writeAfterCloseThrows() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      TableSink sink = ctx.registerStreamingTable("t", oneIntSchema(), 4);
      sink.close();
      try (VectorSchemaRoot b = makeIntBatch(allocator, new int[] {1})) {
        assertThrows(IllegalStateException.class, () -> sink.write(b));
      }
      // close() is idempotent.
      sink.close();
    }
  }

  @Test
  void schemaMismatchRejected() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      // Register with int32; try to write a batch with a different schema (int64 column).
      try (TableSink sink = ctx.registerStreamingTable("t", oneIntSchema(), 4)) {
        Schema otherSchema =
            new Schema(
                Collections.singletonList(
                    new Field("id", FieldType.notNullable(new ArrowType.Int(64, true)), null)));
        try (VectorSchemaRoot bad = VectorSchemaRoot.create(otherSchema, allocator)) {
          BigIntVector v = (BigIntVector) bad.getVector(0);
          v.allocateNew(1);
          v.set(0, 42L);
          v.setValueCount(1);
          bad.setRowCount(1);
          assertThrows(RuntimeException.class, () -> sink.write(bad));
        }
      }
    }
  }

  @Test
  void nullArgumentValidation() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      assertThrows(
          IllegalArgumentException.class,
          () -> ctx.registerStreamingTable(null, oneIntSchema(), 4));
      assertThrows(IllegalArgumentException.class, () -> ctx.registerStreamingTable("t", null, 4));
      assertThrows(
          IllegalArgumentException.class, () -> ctx.registerStreamingTable("t", oneIntSchema(), 0));
      assertThrows(
          IllegalArgumentException.class,
          () -> ctx.registerStreamingTable("t", oneIntSchema(), -1));
      try (TableSink sink = ctx.registerStreamingTable("t", oneIntSchema(), 4)) {
        assertThrows(IllegalArgumentException.class, () -> sink.write(null));
        assertThrows(IllegalArgumentException.class, () -> sink.fail(null));
      }
      // Verify the sink's allocator gets cleaned up implicitly via close().
      assertNotNull(allocator);
    }
  }

  /**
   * Regression stress test for the lost-wakeup race in {@code close()} vs. blocked {@code write()}.
   * With {@code Notify::notify_waiters} alone (the wake is dropped if the writer hasn't registered
   * its {@code Notified} future yet), a writer preempted between cloning the sender and entering
   * {@code select!} would park forever on a full channel after a concurrent {@code close()}. This
   * test runs many iterations with no consumer ever reading, so any occurrence of the race
   * deterministically deadlocks (caught by {@code producer.join(timeout)} returning while the
   * thread is still alive).
   *
   * <p>Each iteration: capacity 1, two writes (second parks on backpressure), {@code close()} from
   * a separate thread immediately after the first write completes, no consumer. The fix (durable
   * {@code closed_flag} re-check after registering the {@code Notified} future) prevents the parked
   * writer from missing the wake.
   */
  @Test
  void closeWakesBlockedWriteUnderStress() throws Exception {
    final int iterations = 100;
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      for (int i = 0; i < iterations; i++) {
        TableSink sink = ctx.registerStreamingTable("t" + i, oneIntSchema(), 1);
        AtomicReference<Throwable> producerError = new AtomicReference<>();
        CountDownLatch readyToBlock = new CountDownLatch(1);
        Thread producer =
            new Thread(
                () -> {
                  try (VectorSchemaRoot b1 = makeIntBatch(allocator, new int[] {1});
                      VectorSchemaRoot b2 = makeIntBatch(allocator, new int[] {2})) {
                    sink.write(b1);
                    readyToBlock.countDown();
                    sink.write(b2); // parks on backpressure; close() must wake it
                  } catch (Throwable t) {
                    producerError.set(t);
                  }
                });
        producer.start();
        readyToBlock.await(5, TimeUnit.SECONDS);
        // Race: close() may run before, during, or after the producer registers its Notified
        // future. With the broken notify_waiters() approach, "before registration" loses the
        // wake. With the closed_flag re-check, all interleavings terminate.
        sink.close();
        producer.join(5_000);
        assertTrue(
            !producer.isAlive(),
            "iteration " + (i + 1) + ": producer did not unblock after close() (lost wakeup)");
        assertNotNull(
            producerError.get(),
            "iteration " + (i + 1) + ": producer's parked write should have thrown");
      }
    }
  }

  /**
   * Regression test for zero-column schemas. {@link TableSink#write} derives its FFI scratch
   * allocator from the batch's first field vector, which means it cannot handle a zero-column
   * batch. Rather than fail at first write (after registration succeeds), {@code
   * registerStreamingTable} rejects empty schemas up front so the broken state is visible to the
   * caller immediately.
   */
  @Test
  void zeroColumnSchemaRejectedAtRegistration() {
    try (SessionContext ctx = new SessionContext()) {
      Schema empty = new Schema(java.util.Collections.emptyList());
      IllegalArgumentException e =
          assertThrows(
              IllegalArgumentException.class, () -> ctx.registerStreamingTable("t", empty, 4));
      assertTrue(
          e.getMessage().contains("at least one column"),
          () -> "expected 'at least one column' in: " + e.getMessage());
    }
  }

  /**
   * Regression test for top-level Schema metadata being dropped during the per-batch FFI import. On
   * the Rust side {@code RecordBatch::from(StructArray)} rebuilds the schema as {@code
   * Schema::new(fields)}, losing the metadata even though it was correctly delivered by the C Data
   * Interface. Without the fields-only comparison + re-attach in {@code TableSinkHandle::write},
   * any caller registering a schema with top-level metadata gets a confusing "schema does not
   * match" error on the first write even though the fields and buffers are identical.
   */
  @Test
  void writeAcceptsSchemaWithTopLevelMetadata() throws Exception {
    java.util.Map<String, String> meta = java.util.Map.of("source", "shard-A", "version", "1");
    Schema schemaWithMeta =
        new Schema(
            Collections.singletonList(
                new Field("id", FieldType.notNullable(new ArrowType.Int(32, true)), null)),
            meta);
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        TableSink sink = ctx.registerStreamingTable("t", schemaWithMeta, 4)) {
      try (VectorSchemaRoot root = VectorSchemaRoot.create(schemaWithMeta, allocator)) {
        IntVector vec = (IntVector) root.getVector(0);
        vec.allocateNew(2);
        vec.set(0, 7);
        vec.set(1, 9);
        vec.setValueCount(2);
        root.setRowCount(2);
        sink.write(root); // would throw "schema does not match" before the fix
      }
      sink.close();
      try (DataFrame df = ctx.sql("SELECT count(*) FROM t");
          ArrowReader reader = df.collect(allocator)) {
        assertTrue(reader.loadNextBatch());
        BigIntVector count = (BigIntVector) reader.getVectorSchemaRoot().getVector(0);
        assertEquals(2L, count.get(0));
      }
    }
  }

  /**
   * Regression test for the cross-lifecycle drop hazard. Closing the {@link SessionContext} before
   * {@link TableSink#close()} should drop the registered table (and its receiver) so that
   * subsequent {@code write()} calls fail with a "consumer side closed" error. Earlier the sink
   * also held an {@code Arc} to the receiver mutex, which kept the receiver alive even after the
   * table was gone -- a producer that tried to write under backpressure would then park forever
   * with no path to wake. The fix is to keep the receiver owned exclusively by the partition
   * stream.
   */
  @Test
  void writeAfterSessionContextClosedFailsPromptly() throws Exception {
    BufferAllocator allocator = new RootAllocator();
    TableSink sink = null;
    try {
      try (SessionContext ctx = new SessionContext()) {
        // capacity 1 + one queued write so the next write would otherwise park.
        sink = ctx.registerStreamingTable("t", oneIntSchema(), 1);
        try (VectorSchemaRoot b1 = makeIntBatch(allocator, new int[] {1})) {
          sink.write(b1);
        }
      } // SessionContext closes here, dropping the registered table and its receiver.
      // The next write must surface an error promptly rather than parking on a defunct channel.
      final TableSink finalSink = sink;
      Thread writer =
          new Thread(
              () -> {
                try (VectorSchemaRoot b2 = makeIntBatch(allocator, new int[] {2})) {
                  finalSink.write(b2);
                } catch (RuntimeException ignored) {
                  // expected: consumer side closed.
                }
              });
      writer.start();
      writer.join(5_000);
      assertTrue(
          !writer.isAlive(),
          "write() parked after the SessionContext (and its receiver) was dropped");
    } finally {
      // Clean up: TableSink owns native resources independent of the session.
      if (sink != null) {
        sink.close();
      }
      allocator.close();
    }
  }

  /**
   * Regression test for dictionary-encoded fields. {@link TableSink#write} calls {@code
   * Data.exportVectorSchemaRoot} with a null {@code DictionaryProvider}; if the registered schema
   * declares any dictionary-encoded field, the export path requires a non-null provider and would
   * otherwise throw NPE on first write. We reject at registration so the breakage is visible up
   * front; lifting this restriction would mean an overload that accepts a {@code
   * DictionaryProvider}, which is out of scope for v1.
   */
  @Test
  void dictionaryEncodedSchemaRejectedAtRegistration() {
    try (SessionContext ctx = new SessionContext()) {
      org.apache.arrow.vector.types.pojo.DictionaryEncoding enc =
          new org.apache.arrow.vector.types.pojo.DictionaryEncoding(
              /* id */ 0L, /* ordered */ false, /* indexType */ null);
      Field dictField =
          new Field(
              "color", new FieldType(true, new ArrowType.Int(32, true), enc), /* children */ null);
      Schema schema = new Schema(Collections.singletonList(dictField));
      IllegalArgumentException e =
          assertThrows(
              IllegalArgumentException.class, () -> ctx.registerStreamingTable("t", schema, 4));
      String msg = e.getMessage();
      assertTrue(
          msg.contains("dictionary-encoded") && msg.contains("color"),
          () -> "expected mention of 'dictionary-encoded' and 'color' in: " + msg);
    }
  }

  /**
   * Regression test for nested dictionary-encoded fields. {@code Data.exportVectorSchemaRoot}
   * recurses into Struct/List/Map/Union children, so a dictionary at any depth would NPE on first
   * write if the validation only inspected top-level fields. The error message must name the dotted
   * path so the caller can find the offending child.
   */
  @Test
  void nestedDictionaryEncodedSchemaRejectedAtRegistration() {
    try (SessionContext ctx = new SessionContext()) {
      org.apache.arrow.vector.types.pojo.DictionaryEncoding enc =
          new org.apache.arrow.vector.types.pojo.DictionaryEncoding(
              /* id */ 7L, /* ordered */ false, /* indexType */ null);
      Field nestedDict =
          new Field("code", new FieldType(true, new ArrowType.Int(32, true), enc), null);
      Field structField =
          new Field(
              "row",
              FieldType.notNullable(new ArrowType.Struct()),
              Collections.singletonList(nestedDict));
      Schema schema = new Schema(Collections.singletonList(structField));
      IllegalArgumentException e =
          assertThrows(
              IllegalArgumentException.class, () -> ctx.registerStreamingTable("t", schema, 4));
      String msg = e.getMessage();
      assertTrue(
          msg.contains("dictionary-encoded") && msg.contains("row.code"),
          () -> "expected mention of 'dictionary-encoded' and 'row.code' in: " + msg);
    }
  }

  /**
   * Regression test for re-entering the Tokio runtime from {@link TableSink#write}. A {@code
   * TableProvider.scan} callback runs on a Tokio worker thread (the executor invokes it from inside
   * the same {@code runtime.block_on} that drives query execution). If {@code write()} naively
   * calls {@code Runtime::block_on} from there, Tokio panics with "Cannot start a runtime from
   * within a runtime". The fix detects an existing runtime context via {@code
   * Handle::try_current()} and uses {@code block_in_place} + {@code Handle::block_on} -- the
   * supported pattern for synchronously waiting on a future from inside a worker.
   *
   * <p>Test shape: a {@link TableProvider} whose {@code scan} (a) returns its own one-row reader
   * (so the trigger query can finish) and (b) writes one batch into a sibling streaming sink. We
   * then SELECT from the trigger table to invoke the scan callback, close the sink, and SELECT the
   * count from the streaming table -- it should be 1, not panic.
   */
  @Test
  void writeFromInsideTableProviderScanDoesNotPanic() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      TableSink streamingSink =
          ctx.registerStreamingTable("streamed", oneIntSchema(), /* capacity */ 4);
      try {
        // Trigger table: schema is a single int "id". Its scan() implementation writes one row
        // into `streamingSink` while DataFusion is on a Tokio worker thread.
        TableProvider trigger =
            new TableProvider() {
              @Override
              public Schema schema() {
                return oneIntSchema();
              }

              @Override
              public org.apache.arrow.vector.ipc.ArrowReader scan(BufferAllocator scanAllocator) {
                // (a) Push one batch into the streaming sink. This is the call that previously
                // panicked with "Cannot start a runtime from within a runtime".
                try (VectorSchemaRoot pushed = makeIntBatch(allocator, new int[] {42})) {
                  streamingSink.write(pushed);
                }
                // (b) Build and serialise a one-row trigger batch as IPC bytes; return a reader
                // backed by those bytes. Same shape as PR #65's InMemoryTableProvider fixture.
                java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
                try (BufferAllocator tmp = new RootAllocator();
                    VectorSchemaRoot root = VectorSchemaRoot.create(oneIntSchema(), tmp)) {
                  IntVector v = (IntVector) root.getVector(0);
                  v.allocateNew(1);
                  v.set(0, 0);
                  v.setValueCount(1);
                  root.setRowCount(1);
                  try (org.apache.arrow.vector.ipc.ArrowStreamWriter writer =
                      new org.apache.arrow.vector.ipc.ArrowStreamWriter(
                          root, null, java.nio.channels.Channels.newChannel(baos))) {
                    writer.start();
                    writer.writeBatch();
                    writer.end();
                  } catch (java.io.IOException e) {
                    throw new RuntimeException(e);
                  }
                }
                return new org.apache.arrow.vector.ipc.ArrowStreamReader(
                    new java.io.ByteArrayInputStream(baos.toByteArray()), scanAllocator);
              }
            };
        ctx.registerTable("trigger", trigger);
        // Drive the scan callback. This runs on a Tokio worker; the inner sink.write must not
        // panic the JVM.
        try (DataFrame df = ctx.sql("SELECT count(*) FROM trigger");
            ArrowReader r = df.collect(allocator)) {
          assertTrue(r.loadNextBatch());
          assertEquals(1L, ((BigIntVector) r.getVectorSchemaRoot().getVector(0)).get(0));
        }
        // Close the sink (no more writes coming).
      } finally {
        streamingSink.close();
      }
      // Now drain the streaming table; we should see the single row written from inside the
      // scan callback.
      try (DataFrame df = ctx.sql("SELECT count(*) FROM streamed");
          ArrowReader r = df.collect(allocator)) {
        assertTrue(r.loadNextBatch());
        assertEquals(1L, ((BigIntVector) r.getVectorSchemaRoot().getVector(0)).get(0));
      }
    }
  }
}
