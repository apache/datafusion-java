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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.datafusion.DataFrame;
import org.apache.datafusion.ExecutedPlan;
import org.apache.datafusion.OperatorMetrics;
import org.apache.datafusion.SessionContext;

/**
 * Run a small SQL query, collect the result, then print the executed physical plan with the
 * per-operator metrics DataFusion populated during execution.
 *
 * <p>Run with:
 *
 * <pre>
 * ./mvnw -pl :datafusion-java-examples exec:exec \
 *     -Dexec.mainClass=org.apache.datafusion.examples.ExecutedPlanExample
 * </pre>
 */
public final class ExecutedPlanExample {

  public static void main(String[] args) throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        DataFrame df =
            ctx.sql(
                "SELECT x % 3 AS bucket, COUNT(*) AS n "
                    + "FROM (VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9)) AS t(x) "
                    + "GROUP BY bucket")) {
      try (ArrowReader reader = df.collect(allocator)) {
        while (reader.loadNextBatch()) {
          // drain to populate the metric counters
        }
      }
      ExecutedPlan plan = df.executedPlan();
      printTree(plan, 0);
    }
  }

  private static void printTree(ExecutedPlan node, int depth) {
    String indent = "  ".repeat(depth);
    System.out.println(indent + node.name());
    if (!node.displayDetails().isEmpty()) {
      System.out.println(indent + "  " + node.displayDetails());
    }
    OperatorMetrics m = node.metrics();
    if (m.outputRows().isPresent()) {
      System.out.println(indent + "  outputRows=" + m.outputRows().getAsLong());
    }
    if (m.elapsedComputeNanos().isPresent()) {
      System.out.println(indent + "  elapsedComputeNanos=" + m.elapsedComputeNanos().getAsLong());
    }
    for (ExecutedPlan child : node.children()) {
      printTree(child, depth + 1);
    }
  }

  private ExecutedPlanExample() {}
}
