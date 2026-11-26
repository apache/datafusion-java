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

import java.util.List;
import java.util.OptionalLong;

import org.apache.datafusion.spark.SchemaConverter.ProjectionColumn;
import org.apache.spark.sql.sources.Filter;
import org.junit.jupiter.api.Test;

/** Verifies the SQL fallback wire, including {@code arrow_cast} injection for cast columns. */
class SqlQueryTest {

  private static final List<Filter> NO_FILTERS = List.of();

  @Test
  void selectStarWhenNoColumns() {
    assertEquals(
        "SELECT * FROM \"t\"", SqlQuery.build("t", null, NO_FILTERS, OptionalLong.empty()));
  }

  @Test
  void castColumnsWrappedInArrowCastAndAliased() {
    List<ProjectionColumn> columns =
        List.of(
            new ProjectionColumn("id", null),
            new ProjectionColumn("channel", "Int32"),
            new ProjectionColumn("ts", "Timestamp(Microsecond)"));
    assertEquals(
        "SELECT \"id\", arrow_cast(\"channel\", 'Int32') AS \"channel\", "
            + "arrow_cast(\"ts\", 'Timestamp(Microsecond)') AS \"ts\" FROM \"t\"",
        SqlQuery.build("t", columns, NO_FILTERS, OptionalLong.empty()));
  }
}
