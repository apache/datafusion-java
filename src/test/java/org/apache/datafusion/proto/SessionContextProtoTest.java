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

package org.apache.datafusion.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.datafusion.DataFrame;
import org.apache.datafusion.SessionContext;
import org.apache.datafusion.protobuf.EmptyRelationNode;
import org.apache.datafusion.protobuf.LogicalExprNode;
import org.apache.datafusion.protobuf.LogicalPlanNode;
import org.apache.datafusion.protobuf.ProjectionNode;
import org.junit.jupiter.api.Test;

import datafusion_common.DatafusionCommon;

class SessionContextProtoTest {

  @Test
  void fromProtoExecutesProjectionOverEmptyRelation() throws Exception {
    LogicalPlanNode plan =
        LogicalPlanNode.newBuilder()
            .setProjection(
                ProjectionNode.newBuilder()
                    .setInput(
                        LogicalPlanNode.newBuilder()
                            .setEmptyRelation(
                                EmptyRelationNode.newBuilder().setProduceOneRow(true).build())
                            .build())
                    .addExpr(
                        LogicalExprNode.newBuilder()
                            .setLiteral(
                                DatafusionCommon.ScalarValue.newBuilder().setInt32Value(1).build())
                            .build())
                    .build())
            .build();

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        DataFrame df = ctx.fromProto(plan.toByteArray());
        ArrowReader reader = df.collect(allocator)) {
      assertTrue(reader.loadNextBatch());
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      assertEquals(1, root.getRowCount());
      IntVector col = (IntVector) root.getVector(0);
      assertEquals(1, col.get(0));
    }
  }
}
