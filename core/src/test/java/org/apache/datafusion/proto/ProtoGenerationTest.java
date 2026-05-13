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

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.datafusion.protobuf.LogicalPlanNode;
import org.junit.jupiter.api.Test;

/**
 * Smoke test: confirms the datafusion-proto schema was downloaded, protoc generated Java sources,
 * those sources landed on the compile classpath, and the {@code protobuf-java} runtime resolves.
 *
 * <p>Does not exercise any DataFusion plan semantics — those tests arrive with JVM-side plan
 * construction.
 */
class ProtoGenerationTest {

  @Test
  void generatedClassIsLoadableAndConstructible() {
    LogicalPlanNode node = LogicalPlanNode.newBuilder().build();
    assertNotNull(node);
  }
}
