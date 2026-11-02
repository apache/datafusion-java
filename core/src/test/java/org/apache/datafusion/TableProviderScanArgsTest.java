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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.datafusion.protobuf.LogicalExprNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class TableProviderScanArgsTest {
  private static final Schema SCHEMA =
      new Schema(
          List.of(
              Field.nullable("id", new ArrowType.Int(32, true)),
              Field.nullable("name", ArrowType.Utf8.INSTANCE)));

  /** A selective source, not a SQL evaluator: only integer id equality/ranges are supported. */
  private static class SelectiveSource implements TableProvider {
    final FilterPushdown capability;
    final List<ScanArgs> requests = Collections.synchronizedList(new ArrayList<>());
    final List<Integer> idsRead = Collections.synchronizedList(new ArrayList<>());
    final AtomicInteger streams = new AtomicInteger();
    final List<Integer> partitions = Collections.synchronizedList(new ArrayList<>());

    SelectiveSource(FilterPushdown capability) {
      this.capability = capability;
    }

    @Override
    public Schema schema() {
      return SCHEMA;
    }

    @Override
    public List<FilterPushdown> supportsFiltersPushdown(List<LogicalExprNode> filters) {
      return filters.stream()
          .map(filter -> supported(filter) ? capability : FilterPushdown.UNSUPPORTED)
          .toList();
    }

    private boolean supported(LogicalExprNode filter) {
      if (!filter.hasBinaryExpr()) {
        return false;
      }
      var binary = filter.getBinaryExpr();
      return binary.getOperandsCount() == 2
          && binary.getOperands(0).hasColumn()
          && binary.getOperands(0).getColumn().getName().equals("id")
          && binary.getOperands(1).hasLiteral()
          && (binary.getOp().equals("Eq") || binary.getOp().equals("GtEq"));
    }

    @Override
    public ExecutionPlan scanWithArgs(ScanArgs request) {
      requests.add(request);
      return (partition, allocator) -> {
        partitions.add(partition);
        return read(request, allocator);
      };
    }

    ArrowReader read(ScanArgs request, BufferAllocator allocator) {
      streams.incrementAndGet();
      IntStream selected = IntStream.rangeClosed(1, 4);
      if (capability == FilterPushdown.EXACT) {
        for (LogicalExprNode filter : request.filters()) {
          var binary = filter.getBinaryExpr();
          var literal = binary.getOperands(1).getLiteral();
          long key = literal.hasInt32Value() ? literal.getInt32Value() : literal.getInt64Value();
          selected = selected.filter(id -> binary.getOp().equals("Eq") ? id == key : id >= key);
        }
      }
      // INEXACT deliberately includes false positives. A wrongly pushed LIMIT would lose rows.
      if (request.limit().isPresent()) {
        selected = selected.limit(request.limit().getAsLong());
      }
      List<Integer> ids = selected.boxed().toList();
      idsRead.addAll(ids);
      List<Integer> projection = request.projection().orElse(List.of(0, 1));
      Schema projected =
          new Schema(projection.stream().map(i -> SCHEMA.getFields().get(i)).toList());
      try (VectorSchemaRoot batch = VectorSchemaRoot.create(projected, allocator)) {
        batch.allocateNew();
        for (int column = 0; column < projection.size(); column++) {
          for (int row = 0; row < ids.size(); row++) {
            if (projection.get(column) == 0) {
              ((IntVector) batch.getVector(column)).setSafe(row, ids.get(row));
            } else {
              String name = String.valueOf((char) ('a' + ids.get(row) - 1));
              ((VarCharVector) batch.getVector(column))
                  .setSafe(row, name.getBytes(StandardCharsets.UTF_8));
            }
          }
        }
        batch.setRowCount(ids.size());
        byte[] bytes =
            TableProviderTest.InMemoryTableProvider.serializeBatches(projected, List.of(batch));
        return new ArrowStreamReader(new ByteArrayInputStream(bytes), allocator);
      }
    }
  }

  private static List<List<Object>> collect(DataFrame frame, BufferAllocator allocator)
      throws Exception {
    List<List<Object>> rows = new ArrayList<>();
    try (ArrowReader reader = frame.collect(allocator)) {
      while (reader.loadNextBatch()) {
        var batch = reader.getVectorSchemaRoot();
        for (int index = 0; index < batch.getRowCount(); index++) {
          List<Object> row = new ArrayList<>();
          for (var vector : batch.getFieldVectors()) {
            Object value = vector.getObject(index);
            row.add(value instanceof org.apache.arrow.vector.util.Text ? value.toString() : value);
          }
          rows.add(row);
        }
      }
    }
    return rows;
  }

  @Test
  void exactLookupIsPlannedWithoutReadingAndProjectsAtTheSource() throws Exception {
    var source = new SelectiveSource(TableProvider.FilterPushdown.EXACT);
    try (var allocator = new RootAllocator();
        var context = new SessionContext()) {
      context.registerTable("lookup", source);
      try (var frame = context.sql("SELECT name FROM lookup WHERE id = 2")) {
        try (var explain = frame.explain(false, false)) {
          String plan = collect(explain, allocator).toString();
          assertTrue(plan.contains("JavaScanExec"));
        }
        assertFalse(source.requests.isEmpty());
        assertEquals(0, source.streams.get());
        assertEquals(List.of(List.of("b")), collect(frame, allocator));
      }
      assertEquals(List.of(2), source.idsRead);
      assertEquals(List.of(0), source.partitions);
      ScanArgs request = source.requests.get(source.requests.size() - 1);
      assertEquals(Optional.of(List.of(1)), request.projection());
      assertEquals(1, request.filters().size());
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = TableProvider.FilterPushdown.class,
      names = {"INEXACT", "UNSUPPORTED"})
  void residualFilterRunsBeforeLimit(TableProvider.FilterPushdown capability) throws Exception {
    var source = new SelectiveSource(capability);
    try (var allocator = new RootAllocator();
        var context = new SessionContext()) {
      context.registerTable("source", source);
      try (var frame = context.sql("SELECT id FROM source WHERE id >= 3 LIMIT 1")) {
        assertEquals(List.of(List.of(3)), collect(frame, allocator));
      }
      assertTrue(source.requests.stream().allMatch(r -> r.limit().isEmpty()));
      assertEquals(List.of(1, 2, 3, 4), source.idsRead);
      if (capability == TableProvider.FilterPushdown.UNSUPPORTED) {
        assertTrue(source.requests.stream().allMatch(r -> r.filters().isEmpty()));
      } else {
        assertTrue(source.requests.stream().anyMatch(r -> !r.filters().isEmpty()));
      }
    }
  }

  @Test
  void exactFilterAllowsLimitPushdown() throws Exception {
    var source = new SelectiveSource(TableProvider.FilterPushdown.EXACT);
    try (var allocator = new RootAllocator();
        var context = new SessionContext()) {
      context.registerTable("source", source);
      try (var frame = context.sql("SELECT name FROM source WHERE id >= 2 LIMIT 1")) {
        assertEquals(List.of(List.of("b")), collect(frame, allocator));
      }
      assertEquals(List.of(2), source.idsRead);
      assertTrue(source.requests.stream().anyMatch(r -> r.limit().orElse(-1) == 1));
    }
  }

  @Test
  void emptyProjectionPreservesRowsForCountStar() throws Exception {
    var source = new SelectiveSource(TableProvider.FilterPushdown.EXACT);
    try (var allocator = new RootAllocator();
        var context = new SessionContext()) {
      context.registerTable("source", source);
      try (var frame = context.sql("SELECT COUNT(*) FROM source")) {
        assertEquals(List.of(List.of(4L)), collect(frame, allocator));
      }
      assertTrue(
          source.requests.stream().anyMatch(r -> r.projection().equals(Optional.of(List.of()))));
    }
  }

  @Test
  void independentPlannedScansKeepTheirOwnFilterAndProjection() throws Exception {
    var source = new SelectiveSource(TableProvider.FilterPushdown.EXACT);
    try (var allocator = new RootAllocator();
        var context = new SessionContext()) {
      context.registerTable("source", source);
      try (var frame =
          context.sql(
              "SELECT name FROM source WHERE id = 1 UNION ALL SELECT name FROM source WHERE id = 4"
                  + " ORDER BY name")) {
        assertEquals(List.of(List.of("a"), List.of("d")), collect(frame, allocator));
      }
      assertEquals(2, source.streams.get());
      assertEquals(Set.of(1, 4), Set.copyOf(source.idsRead));
    }
  }

  @Test
  void sourceCanRejectAnUnboundedAccessPathDuringPlanning() {
    var source =
        new SelectiveSource(TableProvider.FilterPushdown.EXACT) {
          @Override
          public ExecutionPlan scanWithArgs(ScanArgs request) {
            if (request.filters().isEmpty()) {
              throw new UnsupportedOperationException("A bound lookup key is required");
            }
            return super.scanWithArgs(request);
          }
        };
    try (var allocator = new RootAllocator();
        var context = new SessionContext()) {
      context.registerTable("source", source);
      RuntimeException error =
          assertThrows(
              RuntimeException.class,
              () -> {
                try (var frame = context.sql("SELECT name FROM source");
                    var reader = frame.collect(allocator)) {
                  reader.loadNextBatch();
                }
              });
      assertTrue(error.getMessage().contains("A bound lookup key is required"));
      assertEquals(0, source.streams.get());
    }
  }

  @Test
  void malformedFilterCapabilitiesFailWithoutOpeningAStream() {
    var source =
        new SelectiveSource(TableProvider.FilterPushdown.EXACT) {
          @Override
          public List<FilterPushdown> supportsFiltersPushdown(List<LogicalExprNode> filters) {
            return List.of();
          }
        };
    try (var allocator = new RootAllocator();
        var context = new SessionContext()) {
      context.registerTable("source", source);
      RuntimeException error =
          assertThrows(
              RuntimeException.class,
              () -> {
                try (var frame = context.sql("SELECT id FROM source WHERE id = 1");
                    var reader = frame.collect(allocator)) {
                  reader.loadNextBatch();
                }
              });
      assertTrue(error.getMessage().contains("one capability per filter"));
      assertEquals(0, source.streams.get());
    }
  }
}
