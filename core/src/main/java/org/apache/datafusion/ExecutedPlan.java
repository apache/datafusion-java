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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import org.apache.datafusion.protobuf.ExecutedPlanNodeProto;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Snapshot of a DataFusion physical {@code ExecutionPlan} node, including its children and the
 * metrics gathered during execution.
 *
 * <p>{@link DataFrame#executedPlan()} returns the root after the DataFrame has been executed via
 * {@link DataFrame#collect(org.apache.arrow.memory.BufferAllocator)} or {@link
 * DataFrame#executeStream(org.apache.arrow.memory.BufferAllocator)}.
 *
 * @param name short upstream name, e.g. {@code "AggregateExec"} / {@code "DataSourceExec"}.
 * @param displayDetails one-line {@code DisplayAs::Default} rendering — useful to print or log.
 * @param children children of this node, in upstream order.
 * @param metrics aggregated-across-partitions metrics for this node.
 */
public record ExecutedPlan(
    String name, String displayDetails, List<ExecutedPlan> children, OperatorMetrics metrics) {

  /**
   * Decode the protobuf bytes returned by {@code executedPlanNative}. Internal — package-private
   * for testing the codec independently of JNI.
   */
  static ExecutedPlan fromBytes(byte[] bytes) {
    ExecutedPlanNodeProto root;
    try {
      root = ExecutedPlanNodeProto.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Failed to decode ExecutedPlanNodeProto", e);
    }
    return fromProto(root);
  }

  private static ExecutedPlan fromProto(ExecutedPlanNodeProto proto) {
    List<ExecutedPlan> children = new ArrayList<>(proto.getChildrenCount());
    for (int i = 0; i < proto.getChildrenCount(); i++) {
      children.add(fromProto(proto.getChildren(i)));
    }
    OperatorMetrics metrics =
        new OperatorMetrics(
            optionalLong(proto.hasOutputRows(), proto.getOutputRows()),
            optionalLong(proto.hasElapsedComputeNanos(), proto.getElapsedComputeNanos()),
            optionalLong(proto.hasOutputBytes(), proto.getOutputBytes()),
            optionalLong(proto.hasOutputBatches(), proto.getOutputBatches()),
            optionalLong(proto.hasSpillCount(), proto.getSpillCount()),
            optionalLong(proto.hasSpilledBytes(), proto.getSpilledBytes()),
            optionalLong(proto.hasSpilledRows(), proto.getSpilledRows()),
            optionalLong(proto.hasCurrentMemoryUsage(), proto.getCurrentMemoryUsage()),
            Map.copyOf(proto.getCustomCountersMap()));
    return new ExecutedPlan(
        proto.getName(), proto.getDisplayDetails(), List.copyOf(children), metrics);
  }

  private static OptionalLong optionalLong(boolean present, long value) {
    return present ? OptionalLong.of(value) : OptionalLong.empty();
  }
}
