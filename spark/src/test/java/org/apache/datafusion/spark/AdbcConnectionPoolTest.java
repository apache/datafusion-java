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

package org.apache.datafusion.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Unit + concurrency tests for {@link AdbcConnectionPool}; no native driver required. */
class AdbcConnectionPoolTest {

  @AfterEach
  void tearDown() {
    AdbcConnectionPool.resetForTesting();
  }

  private static AdbcOptions options(Map<String, String> raw) {
    return AdbcOptions.fromOptions(new CaseInsensitiveStringMap(new LinkedHashMap<>(raw)));
  }

  private static LinkedHashMap<String, String> base() {
    LinkedHashMap<String, String> m = new LinkedHashMap<>();
    m.put("driver", "/path/to/lib.so");
    m.put("table", "t");
    return m;
  }

  // ---- cacheKey() ----

  @Test
  void cacheKeyIgnoresOptionOrder() {
    LinkedHashMap<String, String> a = base();
    a.put("foo", "1");
    a.put("bar", "2");
    LinkedHashMap<String, String> b = base();
    b.put("bar", "2");
    b.put("foo", "1");

    AdbcConnectionPool.Key ka = options(a).cacheKey();
    AdbcConnectionPool.Key kb = options(b).cacheKey();
    assertEquals(ka, kb);
    assertEquals(ka.hashCode(), kb.hashCode());
  }

  @Test
  void cacheKeyDistinguishesDriverAndOptionValues() {
    LinkedHashMap<String, String> a = base();
    a.put("foo", "1");
    LinkedHashMap<String, String> diffValue = base();
    diffValue.put("foo", "2");
    LinkedHashMap<String, String> diffDriver = base();
    diffDriver.put("driver", "/other/lib.so");
    diffDriver.put("foo", "1");

    assertNotEquals(options(a).cacheKey(), options(diffValue).cacheKey());
    assertNotEquals(options(a).cacheKey(), options(diffDriver).cacheKey());
  }

  @Test
  void cacheKeyIgnoresControlKeys() {
    // table and target_partitions are connector-control, not forwarded to the native database,
    // so they must not affect the cache key.
    LinkedHashMap<String, String> a = base();
    a.put("target_partitions", "4");
    LinkedHashMap<String, String> b = base();
    b.put("table", "other-table");
    b.put("target_partitions", "16");

    assertEquals(options(a).cacheKey(), options(b).cacheKey());
  }

  // ---- computeIfAbsent: at-most-once build under concurrency ----

  @Test
  void concurrentAcquireBuildsExactlyOnce() throws Exception {
    AtomicInteger builds = new AtomicInteger();
    AdbcConnectionPool.setBuilderForTesting(
        (key, opts) -> {
          builds.incrementAndGet();
          return new AdbcConnectionPool.CachedDatabase(
              new RootAllocator(), mock(AdbcDatabase.class));
        });

    AdbcOptions opts = options(base());
    int threads = 16;
    CyclicBarrier barrier = new CyclicBarrier(threads);
    ConcurrentLinkedQueue<AdbcConnectionPool.Lease> leases = new ConcurrentLinkedQueue<>();
    ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

    runConcurrently(
        threads,
        () -> {
          try {
            barrier.await();
            leases.add(AdbcConnectionPool.acquire(opts));
          } catch (Throwable t) {
            errors.add(t);
          }
        });

    assertTrue(errors.isEmpty(), () -> "unexpected errors: " + errors);
    assertEquals(1, builds.get(), "database built exactly once for one key");
    assertEquals(1, AdbcConnectionPool.cacheSizeForTesting());
    assertEquals(threads, leases.size());
    for (AdbcConnectionPool.Lease lease : leases) {
      lease.close();
    }
  }

  @Test
  void distinctKeysBuildDistinctEntries() throws Exception {
    AdbcConnectionPool.setBuilderForTesting(
        (key, opts) ->
            new AdbcConnectionPool.CachedDatabase(new RootAllocator(), mock(AdbcDatabase.class)));

    LinkedHashMap<String, String> a = base();
    a.put("foo", "1");
    LinkedHashMap<String, String> b = base();
    b.put("foo", "2");

    AdbcConnectionPool.Lease la = AdbcConnectionPool.acquire(options(a));
    AdbcConnectionPool.Lease lb = AdbcConnectionPool.acquire(options(b));
    assertEquals(2, AdbcConnectionPool.cacheSizeForTesting());
    la.close();
    lb.close();
  }

  @Test
  void buildFailureSurfacesAdbcExceptionAndLeavesCacheEmpty() throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    AdbcConnectionPool.setBuilderForTesting(
        (key, opts) -> {
          attempts.incrementAndGet();
          throw AdbcException.io("boom");
        });

    AdbcOptions opts = options(base());
    int threads = 8;
    CyclicBarrier barrier = new CyclicBarrier(threads);
    ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

    runConcurrently(
        threads,
        () -> {
          try {
            barrier.await();
            AdbcConnectionPool.acquire(opts);
          } catch (AdbcException e) {
            errors.add(e); // expected
          } catch (Throwable t) {
            errors.add(t);
          }
        });

    assertEquals(threads, errors.size());
    for (Throwable t : errors) {
      assertTrue(t instanceof AdbcException, () -> "expected AdbcException, got " + t);
    }
    assertEquals(0, AdbcConnectionPool.cacheSizeForTesting(), "failed build leaves no entry");

    // A subsequent good build inserts exactly one entry (retry works).
    AdbcConnectionPool.setBuilderForTesting(
        (key, o) ->
            new AdbcConnectionPool.CachedDatabase(new RootAllocator(), mock(AdbcDatabase.class)));
    AdbcConnectionPool.Lease lease = AdbcConnectionPool.acquire(opts);
    assertEquals(1, AdbcConnectionPool.cacheSizeForTesting());
    lease.close();
  }

  // ---- Lease close semantics ----

  @Test
  void leaseCloseClosesTaskConnectionButNotDatabase() throws Exception {
    try (BufferAllocator root = new RootAllocator()) {
      AdbcDatabase db = mock(AdbcDatabase.class);
      AdbcConnection taskConn = mock(AdbcConnection.class);
      AdbcConnectionPool.CachedDatabase cached = new AdbcConnectionPool.CachedDatabase(root, db);
      BufferAllocator child = root.newChildAllocator("task", 0, Long.MAX_VALUE);

      AdbcConnectionPool.Lease lease = new AdbcConnectionPool.Lease(cached, child, taskConn);
      lease.close();

      verify(taskConn, times(1)).close();
      verify(db, never()).close();
    }
  }

  // ---- acquire opens a per-task connection off the one cached database ----

  @Test
  void acquireOpensConnectionPerTask() throws Exception {
    AdbcDatabase db = mock(AdbcDatabase.class);
    when(db.connect()).thenAnswer(invocation -> mock(AdbcConnection.class));
    AdbcConnectionPool.setBuilderForTesting(
        (key, opts) -> new AdbcConnectionPool.CachedDatabase(new RootAllocator(), db));

    AdbcOptions opts = options(base());
    AdbcConnectionPool.Lease l1 = AdbcConnectionPool.acquire(opts);
    AdbcConnectionPool.Lease l2 = AdbcConnectionPool.acquire(opts);

    assertEquals(1, AdbcConnectionPool.cacheSizeForTesting(), "one cached database");
    verify(db, times(2)).connect(); // one connection per task
    assertNotEquals(l1.connection(), l2.connection());
    l1.close();
    l2.close();
  }

  private static void runConcurrently(int threads, Runnable body) throws InterruptedException {
    Thread[] pool = new Thread[threads];
    for (int i = 0; i < threads; i++) {
      pool[i] = new Thread(body, "acquire-" + i);
      pool[i].start();
    }
    for (Thread t : pool) {
      t.join();
    }
  }
}
