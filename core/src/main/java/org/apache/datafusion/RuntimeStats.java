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
 * Snapshot of the underlying Tokio runtime from {@link SessionContext#runtimeStats()}. Mirrors the
 * subset of {@code tokio_metrics::RuntimeMetrics} fields that node-level dashboards typically
 * surface (worker count, busy time, queue depth, etc.).
 *
 * <p>Instances are immutable. The snapshot is process-wide rather than per-session because the JNI
 * library drives a single shared multi-threaded Tokio runtime; gating on the {@code SessionContext}
 * only ensures the caller still has a live handle.
 *
 * <p>Requires the {@code runtime-metrics} Cargo feature on the native crate, plus {@code
 * RUSTFLAGS="--cfg tokio_unstable"} at build time.
 */
public final class RuntimeStats {
  private final int numWorkers;
  private final long liveTasksCount;
  private final long globalQueueDepth;
  private final long elapsedNanos;
  private final long totalBusyNanos;
  private final long totalParkCount;
  private final long totalPollsCount;
  private final long totalNoopCount;
  private final long totalStealCount;
  private final long totalLocalScheduleCount;
  private final long totalOverflowCount;

  public RuntimeStats(
      int numWorkers,
      long liveTasksCount,
      long globalQueueDepth,
      long elapsedNanos,
      long totalBusyNanos,
      long totalParkCount,
      long totalPollsCount,
      long totalNoopCount,
      long totalStealCount,
      long totalLocalScheduleCount,
      long totalOverflowCount) {
    this.numWorkers = numWorkers;
    this.liveTasksCount = liveTasksCount;
    this.globalQueueDepth = globalQueueDepth;
    this.elapsedNanos = elapsedNanos;
    this.totalBusyNanos = totalBusyNanos;
    this.totalParkCount = totalParkCount;
    this.totalPollsCount = totalPollsCount;
    this.totalNoopCount = totalNoopCount;
    this.totalStealCount = totalStealCount;
    this.totalLocalScheduleCount = totalLocalScheduleCount;
    this.totalOverflowCount = totalOverflowCount;
  }

  /** Number of OS worker threads driving the multi-threaded Tokio runtime. */
  public int numWorkers() {
    return numWorkers;
  }

  /** Number of tasks currently scheduled (alive) on the runtime. */
  public long liveTasksCount() {
    return liveTasksCount;
  }

  /** Tasks waiting in the global injection queue, not yet picked up by a worker. */
  public long globalQueueDepth() {
    return globalQueueDepth;
  }

  /** Wall-clock nanoseconds covered by this snapshot's interval (since runtime start). */
  public long elapsedNanos() {
    return elapsedNanos;
  }

  /** Total nanoseconds workers spent doing work (sum across workers). */
  public long totalBusyNanos() {
    return totalBusyNanos;
  }

  /** Times a worker has parked itself (gone idle waiting for work). */
  public long totalParkCount() {
    return totalParkCount;
  }

  /** Total task polls completed across workers. */
  public long totalPollsCount() {
    return totalPollsCount;
  }

  /** Times a worker unparked but found no work (false wakeup). */
  public long totalNoopCount() {
    return totalNoopCount;
  }

  /** Times a worker successfully stole tasks from another worker. */
  public long totalStealCount() {
    return totalStealCount;
  }

  /** Tasks scheduled into a worker's local queue. */
  public long totalLocalScheduleCount() {
    return totalLocalScheduleCount;
  }

  /** Times a worker's local queue overflowed and pushed tasks to the injector. */
  public long totalOverflowCount() {
    return totalOverflowCount;
  }

  @Override
  public String toString() {
    return "RuntimeStats{numWorkers="
        + numWorkers
        + ", liveTasksCount="
        + liveTasksCount
        + ", globalQueueDepth="
        + globalQueueDepth
        + ", elapsedNanos="
        + elapsedNanos
        + ", totalBusyNanos="
        + totalBusyNanos
        + ", totalParkCount="
        + totalParkCount
        + ", totalPollsCount="
        + totalPollsCount
        + ", totalNoopCount="
        + totalNoopCount
        + ", totalStealCount="
        + totalStealCount
        + ", totalLocalScheduleCount="
        + totalLocalScheduleCount
        + ", totalOverflowCount="
        + totalOverflowCount
        + "}";
  }
}
