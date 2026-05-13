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

package org.apache.datafusion.examples;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.datafusion.DataFrame;
import org.apache.datafusion.SessionContext;
import org.apache.datafusion.protobuf.EmptyRelationNode;
import org.apache.datafusion.protobuf.LogicalExprNode;
import org.apache.datafusion.protobuf.LogicalPlanNode;
import org.apache.datafusion.protobuf.ProjectionNode;

import datafusion_common.DatafusionCommon;

/**
 * Builds a DataFusion {@code LogicalPlanNode} directly via the generated protobuf classes and
 * executes it through {@link SessionContext#fromProto(byte[])}. Useful as a starting point for
 * tools that produce DataFusion plans externally.
 */
public final class ProtoPlanExample {

  private ProtoPlanExample() {}

  public static void main(String[] args) throws Exception {
    // Project two literals over an empty-but-one-row relation: SELECT 42, 7
    LogicalPlanNode plan =
        LogicalPlanNode.newBuilder()
            .setProjection(
                ProjectionNode.newBuilder()
                    .setInput(
                        LogicalPlanNode.newBuilder()
                            .setEmptyRelation(
                                EmptyRelationNode.newBuilder().setProduceOneRow(true).build())
                            .build())
                    .addExpr(literalInt(42))
                    .addExpr(literalInt(7))
                    .build())
            .build();

    try (var allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        DataFrame df = ctx.fromProto(plan.toByteArray());
        ArrowReader reader = df.collect(allocator)) {
      while (reader.loadNextBatch()) {
        VectorSchemaRoot batch = reader.getVectorSchemaRoot();
        System.out.println(batch.contentToTSVString());
      }
    }
  }

  private static LogicalExprNode literalInt(int value) {
    return LogicalExprNode.newBuilder()
        .setLiteral(DatafusionCommon.ScalarValue.newBuilder().setInt32Value(value).build())
        .build();
  }
}
