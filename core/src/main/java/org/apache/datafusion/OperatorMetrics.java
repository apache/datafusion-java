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

import java.util.Map;
import java.util.OptionalLong;

/**
 * Per-operator metrics captured from a populated DataFusion {@code ExecutionPlan} node.
 *
 * <p>Each well-known field is an {@link OptionalLong}: {@link OptionalLong#empty()} means the
 * operator does not track this metric at all (different from {@code 0}, which means "tracked and
 * the answer was zero" — itself a useful signal, e.g. zero spills on a sort that didn't have to
 * spill).
 *
 * <p>{@code customCounters} carries any {@code MetricValue::Count(name)} entries the operator emits
 * whose name doesn't match a well-known variant. Stable Java contract: if upstream DataFusion adds
 * a new built-in metric, it surfaces here automatically without an API change to this record.
 * Values are summed across partitions (matching upstream's {@code MetricsSet::aggregate_by_name}).
 *
 * @param outputRows total rows produced by the operator across all partitions.
 * @param elapsedComputeNanos total wall-clock CPU time spent in this operator across partitions.
 * @param outputBytes total bytes produced by the operator (where tracked).
 * @param outputBatches total {@code RecordBatch} count produced by the operator.
 * @param spillCount number of spill events.
 * @param spilledBytes total bytes spilled to disk.
 * @param spilledRows total rows spilled to disk.
 * @param currentMemoryUsage latest memory-pool reservation reported by this operator.
 * @param customCounters custom {@code Count}-shaped metrics, keyed by upstream metric name.
 */
public record OperatorMetrics(
    OptionalLong outputRows,
    OptionalLong elapsedComputeNanos,
    OptionalLong outputBytes,
    OptionalLong outputBatches,
    OptionalLong spillCount,
    OptionalLong spilledBytes,
    OptionalLong spilledRows,
    OptionalLong currentMemoryUsage,
    Map<String, Long> customCounters) {

  /** Empty metrics — used when the operator's {@code metrics()} returned {@code None}. */
  public static final OperatorMetrics EMPTY =
      new OperatorMetrics(
          OptionalLong.empty(),
          OptionalLong.empty(),
          OptionalLong.empty(),
          OptionalLong.empty(),
          OptionalLong.empty(),
          OptionalLong.empty(),
          OptionalLong.empty(),
          OptionalLong.empty(),
          Map.of());
}
