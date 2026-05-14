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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

class ScalarUdfTest {

  /** Adds 1 to each row of an Int32 column. */
  static final class AddOne implements ScalarUdf {
    @Override
    public FieldVector evaluate(BufferAllocator allocator, List<FieldVector> args) {
      IntVector in = (IntVector) args.get(0);
      IntVector out = new IntVector("add_one_out", allocator);
      int n = in.getValueCount();
      out.allocateNew(n);
      for (int i = 0; i < n; i++) {
        if (in.isNull(i)) {
          out.setNull(i);
        } else {
          out.set(i, in.get(i) + 1);
        }
      }
      out.setValueCount(n);
      return out;
    }
  }

  @Test
  void addOne_overConstantTable_returnsIncrementedValues() throws Exception {
    try (SessionContext ctx = new SessionContext();
        BufferAllocator allocator = new RootAllocator()) {
      ctx.registerUdf(
          "add_one",
          new AddOne(),
          new ArrowType.Int(32, true),
          List.of(new ArrowType.Int(32, true)),
          Volatility.IMMUTABLE);

      try (DataFrame df =
              ctx.sql(
                  "SELECT add_one(x) AS y"
                      + " FROM (VALUES (CAST(1 AS INT)), (CAST(2 AS INT)), (CAST(3 AS INT)))"
                      + " AS t(x)");
          ArrowReader r = df.collect(allocator)) {
        assertEquals(true, r.loadNextBatch());
        VectorSchemaRoot root = r.getVectorSchemaRoot();
        IntVector y = (IntVector) root.getVector("y");
        assertEquals(3, y.getValueCount());
        assertEquals(2, y.get(0));
        assertEquals(3, y.get(1));
        assertEquals(4, y.get(2));
        assertNotNull(y);
      }
    }
  }

  /** Concatenates two Utf8 columns. */
  static final class Concat implements ScalarUdf {
    @Override
    public FieldVector evaluate(BufferAllocator allocator, List<FieldVector> args) {
      org.apache.arrow.vector.VarCharVector left = (org.apache.arrow.vector.VarCharVector) args.get(0);
      org.apache.arrow.vector.VarCharVector right = (org.apache.arrow.vector.VarCharVector) args.get(1);
      org.apache.arrow.vector.VarCharVector out =
          new org.apache.arrow.vector.VarCharVector("concat_out", allocator);
      int n = left.getValueCount();
      out.allocateNew(n);
      for (int i = 0; i < n; i++) {
        if (left.isNull(i) || right.isNull(i)) {
          out.setNull(i);
        } else {
          byte[] l = left.get(i);
          byte[] r = right.get(i);
          byte[] both = new byte[l.length + r.length];
          System.arraycopy(l, 0, both, 0, l.length);
          System.arraycopy(r, 0, both, l.length, r.length);
          out.setSafe(i, both);
        }
      }
      out.setValueCount(n);
      return out;
    }
  }

  @Test
  void concat_overVarCharColumns_concatenatesValues() throws Exception {
    try (SessionContext ctx = new SessionContext();
        BufferAllocator allocator = new RootAllocator()) {
      ctx.registerUdf(
          "java_concat",
          new Concat(),
          new ArrowType.Utf8(),
          List.of(new ArrowType.Utf8(), new ArrowType.Utf8()),
          Volatility.IMMUTABLE);

      try (DataFrame df =
              ctx.sql(
                  "SELECT java_concat(a, b) AS c FROM (VALUES ('foo','bar'),('hello','!')) AS t(a, b)");
          ArrowReader r = df.collect(allocator)) {
        assertEquals(true, r.loadNextBatch());
        VectorSchemaRoot root = r.getVectorSchemaRoot();
        org.apache.arrow.vector.VarCharVector c =
            (org.apache.arrow.vector.VarCharVector) root.getVector("c");
        assertEquals(2, c.getValueCount());
        assertEquals("foobar", new String(c.get(0)));
        assertEquals("hello!", new String(c.get(1)));
      }
    }
  }

  /** Squares a Float64 column. */
  static final class Square implements ScalarUdf {
    @Override
    public FieldVector evaluate(BufferAllocator allocator, List<FieldVector> args) {
      org.apache.arrow.vector.Float8Vector in = (org.apache.arrow.vector.Float8Vector) args.get(0);
      org.apache.arrow.vector.Float8Vector out =
          new org.apache.arrow.vector.Float8Vector("square_out", allocator);
      int n = in.getValueCount();
      out.allocateNew(n);
      for (int i = 0; i < n; i++) {
        if (in.isNull(i)) {
          out.setNull(i);
        } else {
          double v = in.get(i);
          out.set(i, v * v);
        }
      }
      out.setValueCount(n);
      return out;
    }
  }

  @Test
  void square_overFloat64Column_squaresValues() throws Exception {
    try (SessionContext ctx = new SessionContext();
        BufferAllocator allocator = new RootAllocator()) {
      ctx.registerUdf(
          "java_square",
          new Square(),
          new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE),
          List.of(
              new ArrowType.FloatingPoint(
                  org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)),
          Volatility.IMMUTABLE);

      try (DataFrame df =
              ctx.sql("SELECT java_square(x) AS y FROM (VALUES (2.0),(3.5)) AS t(x)");
          ArrowReader r = df.collect(allocator)) {
        assertEquals(true, r.loadNextBatch());
        VectorSchemaRoot root = r.getVectorSchemaRoot();
        org.apache.arrow.vector.Float8Vector y =
            (org.apache.arrow.vector.Float8Vector) root.getVector("y");
        assertEquals(2, y.getValueCount());
        assertEquals(4.0, y.get(0), 0.0);
        assertEquals(12.25, y.get(1), 0.0);
      }
    }
  }

  @Test
  void addOne_invokedTwiceInOneSession_executesIndependently() throws Exception {
    // Re-running the same UDF query twice exercises that GlobalRefs and JNI state
    // don't accumulate across invocations within a session.
    try (SessionContext ctx = new SessionContext();
        BufferAllocator allocator = new RootAllocator()) {
      ctx.registerUdf(
          "add_one",
          new AddOne(),
          new ArrowType.Int(32, true),
          List.of(new ArrowType.Int(32, true)),
          Volatility.IMMUTABLE);

      for (int run = 0; run < 2; run++) {
        try (DataFrame df = ctx.sql("SELECT add_one(CAST(5 AS INT)) AS y");
            ArrowReader r = df.collect(allocator)) {
          assertEquals(true, r.loadNextBatch());
          IntVector y = (IntVector) r.getVectorSchemaRoot().getVector("y");
          assertEquals(1, y.getValueCount());
          assertEquals(6, y.get(0));
        }
      }
    }
  }

  static final class ReturnsNull implements ScalarUdf {
    @Override
    public FieldVector evaluate(BufferAllocator allocator, List<FieldVector> args) {
      return null;
    }
  }

  @Test
  void udfReturningNull_surfacesIllegalStateException() {
    try (SessionContext ctx = new SessionContext();
        BufferAllocator allocator = new RootAllocator()) {
      ctx.registerUdf(
          "bad_null",
          new ReturnsNull(),
          new ArrowType.Int(32, true),
          List.of(new ArrowType.Int(32, true)),
          Volatility.IMMUTABLE);
      RuntimeException ex =
          org.junit.jupiter.api.Assertions.assertThrows(
              RuntimeException.class,
              () -> {
                try (DataFrame df = ctx.sql("SELECT bad_null(CAST(1 AS INT))");
                    ArrowReader r = df.collect(allocator)) {
                  while (r.loadNextBatch()) {}
                }
              });
      org.junit.jupiter.api.Assertions.assertTrue(
          ex.getMessage().contains("returned null"),
          "expected error to mention 'returned null', got: " + ex.getMessage());
    }
  }

  static final class WrongRowCount implements ScalarUdf {
    @Override
    public FieldVector evaluate(BufferAllocator allocator, List<FieldVector> args) {
      IntVector in = (IntVector) args.get(0);
      IntVector out = new IntVector("out", allocator);
      out.allocateNew(in.getValueCount() + 1); // off by one
      for (int i = 0; i < in.getValueCount() + 1; i++) out.set(i, 0);
      out.setValueCount(in.getValueCount() + 1);
      return out;
    }
  }

  @Test
  void udfReturningWrongRowCount_surfacesIllegalStateException() {
    try (SessionContext ctx = new SessionContext();
        BufferAllocator allocator = new RootAllocator()) {
      ctx.registerUdf(
          "bad_rows",
          new WrongRowCount(),
          new ArrowType.Int(32, true),
          List.of(new ArrowType.Int(32, true)),
          Volatility.IMMUTABLE);
      RuntimeException ex =
          org.junit.jupiter.api.Assertions.assertThrows(
              RuntimeException.class,
              () -> {
                try (DataFrame df = ctx.sql("SELECT bad_rows(CAST(1 AS INT))");
                    ArrowReader r = df.collect(allocator)) {
                  while (r.loadNextBatch()) {}
                }
              });
      org.junit.jupiter.api.Assertions.assertTrue(
          ex.getMessage().contains("expected") && ex.getMessage().contains("rows"),
          "expected error to mention row mismatch, got: " + ex.getMessage());
    }
  }

  static final class WrongType implements ScalarUdf {
    @Override
    public FieldVector evaluate(BufferAllocator allocator, List<FieldVector> args) {
      // Declared return type is Int32; return Float64.
      org.apache.arrow.vector.Float8Vector out =
          new org.apache.arrow.vector.Float8Vector("out", allocator);
      out.allocateNew(args.get(0).getValueCount());
      for (int i = 0; i < args.get(0).getValueCount(); i++) out.set(i, 0.0);
      out.setValueCount(args.get(0).getValueCount());
      return out;
    }
  }

  @Test
  void udfReturningWrongType_surfacesTypeMismatch() {
    try (SessionContext ctx = new SessionContext();
        BufferAllocator allocator = new RootAllocator()) {
      ctx.registerUdf(
          "bad_type",
          new WrongType(),
          new ArrowType.Int(32, true),
          List.of(new ArrowType.Int(32, true)),
          Volatility.IMMUTABLE);
      RuntimeException ex =
          org.junit.jupiter.api.Assertions.assertThrows(
              RuntimeException.class,
              () -> {
                try (DataFrame df = ctx.sql("SELECT bad_type(CAST(1 AS INT))");
                    ArrowReader r = df.collect(allocator)) {
                  while (r.loadNextBatch()) {}
                }
              });
      org.junit.jupiter.api.Assertions.assertTrue(
          ex.getMessage().toLowerCase().contains("type"),
          "expected error to mention type mismatch, got: " + ex.getMessage());
    }
  }

  static final class ThrowsIAE implements ScalarUdf {
    @Override
    public FieldVector evaluate(BufferAllocator allocator, List<FieldVector> args) {
      throw new IllegalArgumentException("custom boom from UDF");
    }
  }

  @Test
  void udfThrowingException_propagatesClassAndMessage() {
    try (SessionContext ctx = new SessionContext();
        BufferAllocator allocator = new RootAllocator()) {
      ctx.registerUdf(
          "boom",
          new ThrowsIAE(),
          new ArrowType.Int(32, true),
          List.of(new ArrowType.Int(32, true)),
          Volatility.IMMUTABLE);
      RuntimeException ex =
          org.junit.jupiter.api.Assertions.assertThrows(
              RuntimeException.class,
              () -> {
                try (DataFrame df = ctx.sql("SELECT boom(CAST(1 AS INT))");
                    ArrowReader r = df.collect(allocator)) {
                  while (r.loadNextBatch()) {}
                }
              });
      String msg = ex.getMessage();
      org.junit.jupiter.api.Assertions.assertTrue(
          msg.contains("IllegalArgumentException"),
          "expected class name in error, got: " + msg);
      org.junit.jupiter.api.Assertions.assertTrue(
          msg.contains("custom boom from UDF"),
          "expected user message in error, got: " + msg);
    }
  }
}
