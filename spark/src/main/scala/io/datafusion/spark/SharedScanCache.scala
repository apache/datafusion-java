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

package io.datafusion.spark

import java.util.concurrent.{ConcurrentHashMap, Executors, ScheduledExecutorService, TimeUnit}

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.ArrowReader

/**
 * Everything the driver resolved that an executor needs to rebuild the shared scan: identity
 * (scanId) plus the exact build inputs (factory, options, projection, filters, pinned config).
 */
final case class SharedScanSpec(
    scanId: String,
    factoryFqcn: String,
    optionsBytes: Array[Byte],
    projectionColumnNames: Array[String],
    filterProtoBytes: Array[Array[Byte]],
    pinnedConfig: PinnedSessionConfig
)

/**
 * What one cached shared-scan entry exposes to readers. Implemented by
 * [[NativeSharedScanResources]] (JNI-backed) and by fakes in tests.
 */
trait SharedScanResources extends AutoCloseable {

  /** Output partition count of the planned physical plan. */
  def partitionCount: Int

  /** Child allocator for one task's reader; closed by the task, before release. */
  def newTaskAllocator(name: String): BufferAllocator

  /** Open an independent stream over one plan partition. Concurrent-safe. */
  def openPartitionStream(partition: Int, taskAllocator: BufferAllocator): ArrowReader
}

/**
 * Executor-JVM cache of shared-scan entries, keyed by the driver-minted scanId.
 *
 * Semantics:
 *   - `acquire` builds the entry exactly once per attempt wave: the first caller builds under
 *     the entry's lock, concurrent callers block and share the result. Each successful acquire
 *     increments a refcount that the caller MUST pair with `release(scanId)`.
 *   - Build failures propagate to the builder AND all waiters of that attempt, and are not
 *     cached: the next acquire rebuilds.
 *   - Eviction closes entries with refcount 0 that have been idle longer than their TTL. The
 *     refcount covers every open reader, so native close never races an in-flight stream.
 *     Acquire after eviction rebuilds — correct, just slower.
 *
 * The cache itself is JNI-free: the entry builder is injected, so tests run without native libs.
 */
final class SharedScanCache(
    buildEntry: SharedScanSpec => SharedScanResources,
    nanoClock: () => Long = () => System.nanoTime()
) {

  /**
   * Per-scanId slot. All state transitions are guarded by `this` (the holder's monitor); the
   * build itself also runs under the monitor, which is what blocks concurrent acquirers of the
   * same scan until the entry exists.
   */
  private final class EntryHolder(spec: SharedScanSpec, idleTtlMs: Long) {
    private var resources: SharedScanResources = _
    private var refCount: Int = 0
    private var lastReleaseNanos: Long = nanoClock()
    private var closed: Boolean = false

    /** Returns the resources with refcount incremented, or None if this holder was evicted. */
    def acquire(): Option[SharedScanResources] = synchronized {
      if (closed) return None
      if (resources == null) {
        resources = buildEntry(spec) // throws -> caller removes holder
      }
      refCount += 1
      Some(resources)
    }

    def release(): Unit = synchronized {
      refCount -= 1
      lastReleaseNanos = nanoClock()
    }

    /** Close if idle past TTL; returns true when this holder is now closed. */
    def closeIfIdle(nowNanos: Long): Boolean = synchronized {
      if (closed) return true
      val idle = refCount == 0 &&
        (nowNanos - lastReleaseNanos) >= TimeUnit.MILLISECONDS.toNanos(idleTtlMs)
      if (idle) forceCloseLocked()
      closed
    }

    def forceClose(): Unit = synchronized { forceCloseLocked() }

    private def forceCloseLocked(): Unit = {
      if (!closed) {
        closed = true
        if (resources != null) {
          val r = resources
          resources = null
          r.close()
        }
      }
    }
  }

  private val entries = new ConcurrentHashMap[String, EntryHolder]()

  def acquire(spec: SharedScanSpec, idleTtlMs: Long): SharedScanResources = {
    while (true) {
      val holder =
        entries.computeIfAbsent(spec.scanId, _ => new EntryHolder(spec, idleTtlMs))
      val acquired =
        try {
          holder.acquire()
        } catch {
          case t: Throwable =>
            // Build failed: drop the holder so the next acquire rebuilds, then propagate.
            entries.remove(spec.scanId, holder)
            throw t
        }
      acquired match {
        case Some(resources) => return resources
        case None =>
          // Holder was evicted between map lookup and acquire; retry with a fresh one.
          entries.remove(spec.scanId, holder)
      }
    }
    throw new IllegalStateException("unreachable")
  }

  def release(scanId: String): Unit = {
    val holder = entries.get(scanId)
    if (holder == null) {
      throw new IllegalStateException(
        s"release($scanId) without a cached entry: unbalanced acquire/release")
    }
    holder.release()
  }

  /** Close and remove every idle-past-TTL entry. Called by the evictor daemon and by tests. */
  private[spark] def evictIdleNow(): Unit = {
    val now = nanoClock()
    entries.forEach { (scanId, holder) =>
      if (holder.closeIfIdle(now)) {
        entries.remove(scanId, holder)
      }
    }
  }

  /** Close everything regardless of refcounts. JVM-shutdown path only. */
  def shutdown(): Unit = {
    entries.forEach { (_, holder) => holder.forceClose() }
    entries.clear()
  }
}

object SharedScanCache {

  /** Evictor period. Short relative to any sane TTL; cheap when the map is empty. */
  private val EvictorPeriodMs = 5000L

  /** JVM singleton used by executor tasks. Lazily started together with its evictor daemon. */
  lazy val global: SharedScanCache = {
    val cache = new SharedScanCache(NativeSharedScanResources.build)
    val evictor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor { r =>
      val t = new Thread(r, "datafusion-shared-scan-evictor")
      t.setDaemon(true)
      t
    }
    evictor.scheduleWithFixedDelay(
      () => cache.evictIdleNow(),
      EvictorPeriodMs,
      EvictorPeriodMs,
      TimeUnit.MILLISECONDS)
    Runtime.getRuntime.addShutdownHook(new Thread(() => cache.shutdown()))
    cache
  }
}
