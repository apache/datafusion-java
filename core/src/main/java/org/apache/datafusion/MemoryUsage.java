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
 * Snapshot of session-wide memory usage from {@link SessionContext#memoryUsage()}. Returns the
 * total bytes currently reserved against the session's {@code MemoryPool} and the peak observed
 * since the session was created.
 *
 * <p>Multi-tenant attribution: place each tenant in its own {@link SessionContext}. Sessions in
 * DataFusion are cheap; one per tenant is the standard pattern. Within a single session, the
 * snapshot is the sum across all in-flight queries' operator reservations -- see {@link
 * SessionContext#memoryUsage()} for the precise definition of what is and is not counted.
 *
 * <p>Instances are immutable.
 */
public final class MemoryUsage {
  private final long currentBytes;
  private final long peakBytes;

  public MemoryUsage(long currentBytes, long peakBytes) {
    this.currentBytes = currentBytes;
    this.peakBytes = peakBytes;
  }

  /** Bytes currently reserved against this session's {@code MemoryPool}. */
  public long currentBytes() {
    return currentBytes;
  }

  /**
   * Maximum value of {@link #currentBytes()} observed since the session was created. Monotonic:
   * never decreases for a given session.
   */
  public long peakBytes() {
    return peakBytes;
  }

  @Override
  public String toString() {
    return "MemoryUsage{currentBytes=" + currentBytes + ", peakBytes=" + peakBytes + "}";
  }
}
