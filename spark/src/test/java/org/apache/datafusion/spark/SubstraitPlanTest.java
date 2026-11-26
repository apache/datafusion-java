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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.OptionalLong;
import java.util.Set;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.StringContains;
import org.junit.jupiter.api.Test;

import io.substrait.relation.Fetch;
import io.substrait.relation.Filter;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;

/**
 * Verifies the Substrait pushdown encoding without needing the native driver: build a plan, then
 * re-parse the proto bytes and assert the relation tree and the standard function extensions.
 */
class SubstraitPlanTest {

  private static Schema schema() {
    return new Schema(
        List.of(
            Field.nullable("id", new ArrowType.Int(64, true)),
            Field.nullable("name", ArrowType.Utf8.INSTANCE)));
  }

  @Test
  void buildsScanFilterProjectFetch() throws Exception {
    List<org.apache.spark.sql.sources.Filter> filters =
        List.of(new GreaterThan("id", 1L), new EqualTo("name", "bob"));
    byte[] bytes = SubstraitPlan.build("t", schema(), List.of("name"), filters, OptionalLong.of(2));

    // Re-parse the proto and walk it back into the substrait-java model.
    io.substrait.proto.Plan proto = io.substrait.proto.Plan.parseFrom(bytes);
    io.substrait.plan.Plan plan = new io.substrait.plan.ProtoPlanConverter().from(proto);

    Rel top = plan.getRoots().get(0).getInput();
    // Fetch(limit) -> Project(projection) -> Filter(predicates) -> NamedScan(table).
    Fetch fetch = (Fetch) top;
    assertEquals(OptionalLong.of(2), fetch.getCount());
    Project project = (Project) fetch.getInput();
    assertEquals(1, project.getExpressions().size()); // only the kept column "name"
    Filter filter = (Filter) project.getInput();
    NamedScan scan = (NamedScan) filter.getInput();
    assertEquals(List.of("t"), scan.getNames());

    // The interop guarantee: predicates resolve against the STANDARD catalog, the
    // same function URIs DataFusion's Substrait consumer recognizes.
    String text = proto.toString();
    assertTrue(text.contains("functions_comparison"), "uses standard comparison functions");
    assertTrue(text.contains("functions_boolean"), "uses standard boolean functions");
  }

  @Test
  void wholeTableScanWhenNoPushdown() throws Exception {
    byte[] bytes = SubstraitPlan.build("t", schema(), null, List.of(), OptionalLong.empty());
    io.substrait.proto.Plan proto = io.substrait.proto.Plan.parseFrom(bytes);
    io.substrait.plan.Plan plan = new io.substrait.plan.ProtoPlanConverter().from(proto);
    assertTrue(plan.getRoots().get(0).getInput() instanceof NamedScan);
  }

  @Test
  void canPushWhitelist() {
    Set<String> columns = Set.of("id", "name");
    assertTrue(SubstraitPlan.canPush(new GreaterThan("id", 1L), columns));
    assertTrue(SubstraitPlan.canPush(new EqualTo("name", "bob"), columns));
    // Unknown column -> not pushable.
    assertFalse(SubstraitPlan.canPush(new GreaterThan("missing", 1L), columns));
    // Unsupported predicate shape -> not pushable.
    assertFalse(SubstraitPlan.canPush(new StringContains("name", "b"), columns));
  }
}
