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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.driver.jni.JniDriver;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;

/**
 * Per-executor-JVM cache of the expensive native ADBC objects, so the task slots on one executor do
 * not each repeat driver load, provider registration, and session setup against the same cdylib.
 *
 * <p>A Spark executor runs many tasks (one per slot) in one long-lived JVM. Each task's {@link
 * AdbcColumnarPartitionReader} needs an {@link AdbcConnection}; opening one per task re-runs the
 * native session construction and -- the costly part -- the driver's {@code ContextInit} provider
 * registration, which the DataFusion ADBC driver runs <em>once per database</em>. This holder keeps
 * one {@link CachedDatabase} per {@link Key} (the inputs that determine the native database; see
 * {@link AdbcOptions#cacheKey()}) for the life of the JVM, and each task opens its own short-lived
 * connection off that shared database.
 *
 * <p><strong>Connections are not shared across tasks.</strong> The arrow-adbc Rust FFI exporter
 * reaches the inner connection on every C call via a {@code &mut} to the shared object with no
 * lock, so two task threads calling into one connection concurrently would alias {@code &mut} --
 * undefined behavior, regardless of the driver being internally {@code Sync}. Caching the database
 * (where {@code ContextInit} runs) already removes the per-task cost, so the connection stays
 * per-task.
 *
 * <p>Cached handles outlive every task and are closed only by a JVM shutdown hook (executors are
 * long-lived). A per-task {@link Lease} hands out a task-owned connection plus a child allocator
 * and, on {@link Lease#close()}, releases only those task-owned handles -- never the cached
 * database or its root allocator.
 */
final class AdbcConnectionPool {

  private static final ConcurrentHashMap<Key, CachedDatabase> CACHE = new ConcurrentHashMap<>();
  private static final AtomicBoolean HOOK_INSTALLED = new AtomicBoolean(false);

  // Observability counters: how many native databases were built and how many per-task connections
  // were opened over the JVM's lifetime. The point of the pool is that the first stays at
  // one-per-executor (provider registration runs once); the second scales with tasks. Read by the
  // driver-gated E2E benchmark.
  private static final AtomicInteger NATIVE_DATABASES = new AtomicInteger();
  private static final AtomicInteger TASK_CONNECTIONS = new AtomicInteger();

  // Indirection so unit/concurrency tests can supply cheap doubles in place of native handles.
  private static volatile DatabaseBuilder builder = AdbcConnectionPool::buildNative;

  private AdbcConnectionPool() {}

  /**
   * Identity of a native ADBC database: the driver path/name plus the database-option map, compared
   * order-insensitively (map order does not change the native database). Derived from {@link
   * AdbcOptions} on the executor with no new wire fields -- both inputs already ride along inside
   * the serializable {@link AdbcOptions}.
   */
  static final class Key {
    private final String driver;
    private final List<Map.Entry<String, String>> sortedOptions;

    Key(String driver, Map<String, String> databaseOptions) {
      this.driver = Objects.requireNonNull(driver, "driver");
      List<Map.Entry<String, String>> entries = new ArrayList<>(databaseOptions.size());
      for (Map.Entry<String, String> e : databaseOptions.entrySet()) {
        entries.add(new AbstractMap.SimpleImmutableEntry<>(e.getKey(), e.getValue()));
      }
      entries.sort(Map.Entry.comparingByKey());
      this.sortedOptions = List.copyOf(entries);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Key)) {
        return false;
      }
      Key k = (Key) o;
      return driver.equals(k.driver) && sortedOptions.equals(k.sortedOptions);
    }

    @Override
    public int hashCode() {
      return Objects.hash(driver, sortedOptions);
    }

    @Override
    public String toString() {
      return "AdbcConnectionPool.Key[driver=" + driver + ", options=" + sortedOptions + "]";
    }
  }

  /** One cached native database plus its root allocator. */
  static final class CachedDatabase implements AutoCloseable {
    final BufferAllocator rootAllocator;
    final AdbcDatabase database;
    final AtomicInteger activeLeases = new AtomicInteger();
    volatile long lastAccessNanos;

    CachedDatabase(BufferAllocator rootAllocator, AdbcDatabase database) {
      this.rootAllocator = rootAllocator;
      this.database = database;
    }

    @Override
    public void close() throws Exception {
      // Close order: database -> allocator (the allocator must outlive the handles opened against
      // it).
      AutoCloseables.close(database, rootAllocator);
    }
  }

  /** Builds the native objects for a key; replaceable in tests. */
  @FunctionalInterface
  interface DatabaseBuilder {
    CachedDatabase build(Key key, AdbcOptions options) throws AdbcException;
  }

  /**
   * A task's borrowed handle on a {@link CachedDatabase}. Carries the task-owned connection the
   * reader uses and a per-task child allocator. {@link #close()} releases only what the task owns;
   * the cached database and root allocator are not touched.
   */
  static final class Lease implements AutoCloseable {
    private final CachedDatabase shared;
    private final BufferAllocator taskAllocator;
    private final AdbcConnection connection;

    Lease(CachedDatabase shared, BufferAllocator taskAllocator, AdbcConnection connection) {
      this.shared = shared;
      this.taskAllocator = taskAllocator;
      this.connection = connection;
    }

    AdbcConnection connection() {
      return connection;
    }

    BufferAllocator allocator() {
      return taskAllocator;
    }

    @Override
    public void close() throws Exception {
      try {
        // Close the task-owned connection and allocator. The cached database and root allocator are
        // never closed here -- only by the JVM shutdown hook.
        AutoCloseables.close(connection, taskAllocator);
      } finally {
        shared.activeLeases.decrementAndGet();
      }
    }
  }

  /** Fetch-or-create the cached database for these options and return a per-task lease. */
  static Lease acquire(AdbcOptions options) throws AdbcException {
    installShutdownHookOnce();
    Key key = options.cacheKey();
    CachedDatabase shared = getOrCreate(key, options);
    shared.activeLeases.incrementAndGet();
    shared.lastAccessNanos = System.nanoTime();

    BufferAllocator taskAllocator = null;
    try {
      // A child of the shared root: per-task accounting/isolation; closed when the lease closes.
      taskAllocator = shared.rootAllocator.newChildAllocator("adbc-task", 0, Long.MAX_VALUE);
      AdbcConnection conn = shared.database.connect();
      TASK_CONNECTIONS.incrementAndGet();
      return new Lease(shared, taskAllocator, conn);
    } catch (Exception e) {
      shared.activeLeases.decrementAndGet();
      if (taskAllocator != null) {
        try {
          taskAllocator.close();
        } catch (Exception suppressed) {
          e.addSuppressed(suppressed);
        }
      }
      if (e instanceof AdbcException) {
        throw (AdbcException) e;
      }
      throw new RuntimeException("failed to acquire ADBC connection lease", e);
    }
  }

  private static CachedDatabase getOrCreate(Key key, AdbcOptions options) throws AdbcException {
    try {
      // computeIfAbsent runs the mapping function at most once per absent key; concurrent callers
      // on the same key block until it completes, so exactly one database is built and ContextInit
      // runs once. AdbcException is checked and cannot escape the mapping Function, so wrap it and
      // unwrap below; on failure nothing is inserted, so the next caller retries cleanly.
      return CACHE.computeIfAbsent(
          key,
          k -> {
            try {
              return builder.build(k, options);
            } catch (AdbcException e) {
              throw new UncheckedAdbcException(e);
            }
          });
    } catch (UncheckedAdbcException e) {
      throw e.cause();
    }
  }

  private static CachedDatabase buildNative(Key key, AdbcOptions options) throws AdbcException {
    BufferAllocator root = new RootAllocator();
    AdbcDatabase db = null;
    try {
      db = new JniDriver(root).open(options.driverParameters());
      NATIVE_DATABASES.incrementAndGet();
      return new CachedDatabase(root, db);
    } catch (Exception e) {
      try {
        AutoCloseables.close(db, root);
      } catch (Exception suppressed) {
        e.addSuppressed(suppressed);
      }
      if (e instanceof AdbcException) {
        throw (AdbcException) e;
      }
      throw new RuntimeException("failed to open ADBC database", e);
    }
  }

  private static void installShutdownHookOnce() {
    if (HOOK_INSTALLED.compareAndSet(false, true)) {
      Runtime.getRuntime()
          .addShutdownHook(new Thread(AdbcConnectionPool::closeAll, "adbc-pool-shutdown"));
    }
  }

  /** Remove and close every cached database. Run by the shutdown hook; best-effort. */
  static void closeAll() {
    for (Key key : new ArrayList<>(CACHE.keySet())) {
      CachedDatabase d = CACHE.remove(key);
      if (d != null) {
        try {
          d.close();
        } catch (Exception e) {
          // Best effort: the JVM is going away. Nothing useful to do with the exception here.
        }
      }
    }
  }

  /** Unchecked carrier so a checked {@link AdbcException} can cross the computeIfAbsent lambda. */
  private static final class UncheckedAdbcException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    UncheckedAdbcException(AdbcException cause) {
      super(cause);
    }

    AdbcException cause() {
      return (AdbcException) getCause();
    }
  }

  // ---- Test hooks (package-private) ----

  static void setBuilderForTesting(DatabaseBuilder b) {
    builder = b;
  }

  static int cacheSizeForTesting() {
    return CACHE.size();
  }

  /** Native databases built over the JVM lifetime; one per executor when the pool works. */
  static int databasesBuiltForTesting() {
    return NATIVE_DATABASES.get();
  }

  /** Per-task connections opened over the JVM lifetime. */
  static int taskConnectionsOpenedForTesting() {
    return TASK_CONNECTIONS.get();
  }

  /** Close and clear all cached state and restore production defaults. For test isolation. */
  static void resetForTesting() {
    closeAll();
    builder = AdbcConnectionPool::buildNative;
    NATIVE_DATABASES.set(0);
    TASK_CONNECTIONS.set(0);
  }
}
