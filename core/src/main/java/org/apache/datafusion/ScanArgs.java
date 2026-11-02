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

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import org.apache.datafusion.protobuf.LogicalExprNode;

/**
 * A physical scan request using DataFusion's projection, expression and limit semantics.
 *
 * <p>Projection indices refer to the registered table schema. An absent projection means all
 * columns; a present empty list means no columns (for example, {@code COUNT(*)}), not no rows.
 * Filters use the same expression protobufs as DataFusion logical plans, not SQL strings.
 *
 * <p>The optional limit is the value supplied by DataFusion's planner. Sources may return more
 * rows; they must not return fewer than requested when that many matching rows are available.
 * DataFusion retains responsibility for deciding whether a limit can safely cross a residual
 * filter. Limits outside the non-negative Java {@code long} range are not forwarded; DataFusion
 * still applies the query's limit.
 */
public record ScanArgs(
    Optional<List<Integer>> projection, List<LogicalExprNode> filters, OptionalLong limit) {
  public ScanArgs {
    projection = projection.map(List::copyOf);
    filters = List.copyOf(filters);
  }
}
