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

  private static final ArrowType INT32 = new ArrowType.Int(32, true);
  private static final ArrowType FLOAT64 =
      new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
  private static final ArrowType UTF8 = new ArrowType.Utf8();

  /** Test-only base that supplies the four metadata getters from constructor args. */
  abstract static class AbstractScalarFunction implements ScalarFunction {
    private final String name;
    private final List<ArrowType> argTypes;
    private final ArrowType returnType;
    private final Volatility volatility;

    AbstractScalarFunction(
        String name, List<ArrowType> argTypes, ArrowType returnType, Volatility volatility) {
      this.name = name;
      this.argTypes = argTypes;
      this.returnType = returnType;
      this.volatility = volatility;
    }

    @Override
    public final String name() {
      return name;
    }

    @Override
    public final List<ArrowType> argTypes() {
      return argTypes;
    }

    @Override
    public final ArrowType returnType() {
      return returnType;
    }

    @Override
    public final Volatility volatility() {
      return volatility;
    }
  }

  /** Adds 1 to each row of an Int32 column. */
  static final class AddOne extends AbstractScalarFunction {
    AddOne() {
      this("add_one", Volatility.IMMUTABLE);
    }

    AddOne(String name, Volatility volatility) {
      super(name, List.of(INT32), INT32, volatility);
    }

    @Override
    public ColumnarValue evaluate(BufferAllocator allocator, ScalarFunctionArgs args) {
      IntVector in = (IntVector) args.args().get(0).vector();
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
      return ColumnarValue.array(out);
    }
  }

  @Test
  void addOne_overConstantTable_returnsIncrementedValues() throws Exception {
    try (SessionContext ctx = new SessionContext();
        BufferAllocator allocator = new RootAllocator()) {
      ctx.registerUdf(new ScalarUdf(new AddOne()));

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
      }
    }
  }

  /** Concatenates two Utf8 columns. */
  static final class Concat extends AbstractScalarFunction {
    Concat() {
      super("java_concat", List.of(UTF8, UTF8), UTF8, Volatility.IMMUTABLE);
    }

    @Override
    public ColumnarValue evaluate(BufferAllocator allocator, ScalarFunctionArgs args) {
      org.apache.arrow.vector.VarCharVector left =
          (org.apache.arrow.vector.VarCharVector) args.args().get(0).vector();
      org.apache.arrow.vector.VarCharVector right =
          (org.apache.arrow.vector.VarCharVector) args.args().get(1).vector();
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
      return ColumnarValue.array(out);
    }
  }

  @Test
  void concat_overVarCharColumns_concatenatesValues() throws Exception {
    try (SessionContext ctx = new SessionContext();
        BufferAllocator allocator = new RootAllocator()) {
      ctx.registerUdf(new ScalarUdf(new Concat()));

      try (DataFrame df =
              ctx.sql(
                  "SELECT java_concat(a, b) AS c FROM (VALUES ('foo','bar'),('hello','!')) AS t(a,"
                      + " b)");
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
  static final class Square extends AbstractScalarFunction {
    Square() {
      super("java_square", List.of(FLOAT64), FLOAT64, Volatility.IMMUTABLE);
    }

    @Override
    public ColumnarValue evaluate(BufferAllocator allocator, ScalarFunctionArgs args) {
      org.apache.arrow.vector.Float8Vector in =
          (org.apache.arrow.vector.Float8Vector) args.args().get(0).vector();
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
      return ColumnarValue.array(out);
    }
  }

  @Test
  void square_overFloat64Column_squaresValues() throws Exception {
    try (SessionContext ctx = new SessionContext();
        BufferAllocator allocator = new RootAllocator()) {
      ctx.registerUdf(new ScalarUdf(new Square()));

      try (DataFrame df = ctx.sql("SELECT java_square(x) AS y FROM (VALUES (2.0),(3.5)) AS t(x)");
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
      ctx.registerUdf(new ScalarUdf(new AddOne()));

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

  static final class ReturnsNull extends AbstractScalarFunction {
    ReturnsNull() {
      super("bad_null", List.of(INT32), INT32, Volatility.IMMUTABLE);
    }

    @Override
    public ColumnarValue evaluate(BufferAllocator allocator, ScalarFunctionArgs args) {
      return null;
    }
  }

  @Test
  void udfReturningNull_surfacesIllegalStateException() {
    try (SessionContext ctx = new SessionContext();
        BufferAllocator allocator = new RootAllocator()) {
      ctx.registerUdf(new ScalarUdf(new ReturnsNull()));
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

  static final class WrongRowCount extends AbstractScalarFunction {
    WrongRowCount() {
      super("bad_rows", List.of(INT32), INT32, Volatility.IMMUTABLE);
    }

    @Override
    public ColumnarValue evaluate(BufferAllocator allocator, ScalarFunctionArgs args) {
      IntVector in = (IntVector) args.args().get(0).vector();
      IntVector out = new IntVector("out", allocator);
      out.allocateNew(in.getValueCount() + 1); // off by one
      for (int i = 0; i < in.getValueCount() + 1; i++) out.set(i, 0);
      out.setValueCount(in.getValueCount() + 1);
      return ColumnarValue.array(out);
    }
  }

  @Test
  void udfReturningWrongRowCount_surfacesIllegalStateException() {
    try (SessionContext ctx = new SessionContext();
        BufferAllocator allocator = new RootAllocator()) {
      ctx.registerUdf(new ScalarUdf(new WrongRowCount()));
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

  static final class WrongType extends AbstractScalarFunction {
    WrongType() {
      super("bad_type", List.of(INT32), INT32, Volatility.IMMUTABLE);
    }

    @Override
    public ColumnarValue evaluate(BufferAllocator allocator, ScalarFunctionArgs args) {
      // Declared return type is Int32; return Float64.
      org.apache.arrow.vector.Float8Vector out =
          new org.apache.arrow.vector.Float8Vector("out", allocator);
      FieldVector in = args.args().get(0).vector();
      out.allocateNew(in.getValueCount());
      for (int i = 0; i < in.getValueCount(); i++) out.set(i, 0.0);
      out.setValueCount(in.getValueCount());
      return ColumnarValue.array(out);
    }
  }

  @Test
  void udfReturningWrongType_surfacesTypeMismatch() {
    try (SessionContext ctx = new SessionContext();
        BufferAllocator allocator = new RootAllocator()) {
      ctx.registerUdf(new ScalarUdf(new WrongType()));
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

  static final class ThrowsIAE extends AbstractScalarFunction {
    ThrowsIAE() {
      super("boom", List.of(INT32), INT32, Volatility.IMMUTABLE);
    }

    @Override
    public ColumnarValue evaluate(BufferAllocator allocator, ScalarFunctionArgs args) {
      throw new IllegalArgumentException("custom boom from UDF");
    }
  }

  @Test
  void udfThrowingException_propagatesClassAndMessage() {
    try (SessionContext ctx = new SessionContext();
        BufferAllocator allocator = new RootAllocator()) {
      ctx.registerUdf(new ScalarUdf(new ThrowsIAE()));
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
          msg.contains("IllegalArgumentException"), "expected class name in error, got: " + msg);
      org.junit.jupiter.api.Assertions.assertTrue(
          msg.contains("custom boom from UDF"), "expected user message in error, got: " + msg);
    }
  }

  @Test
  void twoUdfsInOneSession_bothCallable() throws Exception {
    try (SessionContext ctx = new SessionContext();
        BufferAllocator allocator = new RootAllocator()) {
      ctx.registerUdf(new ScalarUdf(new AddOne()));
      ctx.registerUdf(new ScalarUdf(new Square()));

      try (DataFrame df =
              ctx.sql("SELECT add_one(CAST(10 AS INT)) AS a, java_square(CAST(3 AS DOUBLE)) AS b");
          ArrowReader r = df.collect(allocator)) {
        assertEquals(true, r.loadNextBatch());
        VectorSchemaRoot root = r.getVectorSchemaRoot();
        IntVector a = (IntVector) root.getVector("a");
        org.apache.arrow.vector.Float8Vector b =
            (org.apache.arrow.vector.Float8Vector) root.getVector("b");
        assertEquals(11, a.get(0));
        assertEquals(9.0, b.get(0), 0.0);
      }
    }
  }

  @Test
  void registerSameNameAfterCloseInNewSession_works() throws Exception {
    for (int round = 0; round < 2; round++) {
      try (SessionContext ctx = new SessionContext();
          BufferAllocator allocator = new RootAllocator()) {
        ctx.registerUdf(new ScalarUdf(new AddOne()));
        try (DataFrame df = ctx.sql("SELECT add_one(CAST(7 AS INT))");
            ArrowReader r = df.collect(allocator)) {
          assertEquals(true, r.loadNextBatch());
          IntVector v = (IntVector) r.getVectorSchemaRoot().getVector(0);
          assertEquals(8, v.get(0));
        }
      }
    }
  }

  @Test
  void udfAppliedToMultiRowQuery_processesAllRows() throws Exception {
    try (SessionContext ctx = new SessionContext();
        BufferAllocator allocator = new RootAllocator()) {
      ctx.registerUdf(new ScalarUdf(new AddOne()));
      String values =
          java.util.stream.IntStream.rangeClosed(1, 100)
              .mapToObj(i -> "(CAST(" + i + " AS INT))")
              .collect(java.util.stream.Collectors.joining(", "));
      try (DataFrame df =
              ctx.sql("SELECT add_one(x) AS y FROM (VALUES " + values + ") AS t(x) ORDER BY y");
          ArrowReader r = df.collect(allocator)) {
        long total = 0;
        long rows = 0;
        while (r.loadNextBatch()) {
          IntVector y = (IntVector) r.getVectorSchemaRoot().getVector("y");
          for (int i = 0; i < y.getValueCount(); i++) {
            total += y.get(i);
            rows++;
          }
        }
        assertEquals(100, rows);
        // Sum of 2..101 = (2+101)*100/2 = 5150
        assertEquals(5150L, total);
      }
    }
  }

  /**
   * Nullary UDF returning a length-1 Float8 vector. Marked VOLATILE so DataFusion's constant folder
   * does not collapse the call before reaching us. Exercises the path that the abandoned PR #57
   * added a separate rowCount parameter for: a nullary UDF can now broadcast its value through
   * {@link ColumnarValue#scalar(FieldVector)} and the framework handles per-row expansion.
   */
  static final class JavaPi extends AbstractScalarFunction {
    JavaPi() {
      super("java_pi", List.of(), FLOAT64, Volatility.VOLATILE);
    }

    @Override
    public ColumnarValue evaluate(BufferAllocator allocator, ScalarFunctionArgs args) {
      org.apache.arrow.vector.Float8Vector out =
          new org.apache.arrow.vector.Float8Vector("pi_out", allocator);
      out.allocateNew(1);
      out.set(0, Math.PI);
      out.setValueCount(1);
      return ColumnarValue.scalar(out);
    }
  }

  @Test
  void nullaryScalarReturnUdf_overMultiRowQuery_broadcasts() throws Exception {
    try (SessionContext ctx = new SessionContext();
        BufferAllocator allocator = new RootAllocator()) {
      ctx.registerUdf(new ScalarUdf(new JavaPi()));

      try (DataFrame df = ctx.sql("SELECT java_pi() AS p FROM (VALUES (1), (2), (3)) AS t(x)");
          ArrowReader r = df.collect(allocator)) {
        assertEquals(true, r.loadNextBatch());
        VectorSchemaRoot root = r.getVectorSchemaRoot();
        org.apache.arrow.vector.Float8Vector p =
            (org.apache.arrow.vector.Float8Vector) root.getVector("p");
        assertEquals(3, p.getValueCount());
        assertEquals(Math.PI, p.get(0), 0.0);
        assertEquals(Math.PI, p.get(1), 0.0);
        assertEquals(Math.PI, p.get(2), 0.0);
      }
    }
  }

  /**
   * UDF over (int_col, int_literal). On every invocation it asserts that arg 0 is an Array and arg
   * 1 is a Scalar (length-1 vector). Proves the FFI protocol preserves scalar-ness end-to-end
   * rather than materialising the literal to a length-N array on the native side.
   */
  static final class AssertSecondArgIsScalar extends AbstractScalarFunction {
    AssertSecondArgIsScalar() {
      super("assert_scalar_arg", List.of(INT32, INT32), INT32, Volatility.IMMUTABLE);
    }

    @Override
    public ColumnarValue evaluate(BufferAllocator allocator, ScalarFunctionArgs args) {
      if (!(args.args().get(0) instanceof ColumnarValue.Array)) {
        throw new AssertionError(
            "arg 0 expected Array, got " + args.args().get(0).getClass().getSimpleName());
      }
      if (!(args.args().get(1) instanceof ColumnarValue.Scalar)) {
        throw new AssertionError(
            "arg 1 expected Scalar, got " + args.args().get(1).getClass().getSimpleName());
      }
      IntVector left = (IntVector) args.args().get(0).vector();
      IntVector right = (IntVector) args.args().get(1).vector();
      if (right.getValueCount() != 1) {
        throw new AssertionError(
            "Scalar arg vector should have length 1, got " + right.getValueCount());
      }
      int rightVal = right.get(0);
      IntVector out = new IntVector("out", allocator);
      int n = left.getValueCount();
      out.allocateNew(n);
      for (int i = 0; i < n; i++) {
        if (left.isNull(i)) {
          out.setNull(i);
        } else {
          out.set(i, left.get(i) + rightVal);
        }
      }
      out.setValueCount(n);
      return ColumnarValue.array(out);
    }
  }

  @Test
  void scalarLiteralArg_arrivesAsScalarColumnarValue() throws Exception {
    try (SessionContext ctx = new SessionContext();
        BufferAllocator allocator = new RootAllocator()) {
      ctx.registerUdf(new ScalarUdf(new AssertSecondArgIsScalar()));

      try (DataFrame df =
              ctx.sql(
                  "SELECT assert_scalar_arg(x, CAST(100 AS INT)) AS y"
                      + " FROM (VALUES (CAST(1 AS INT)), (CAST(2 AS INT)), (CAST(3 AS INT)))"
                      + " AS t(x)");
          ArrowReader r = df.collect(allocator)) {
        assertEquals(true, r.loadNextBatch());
        VectorSchemaRoot root = r.getVectorSchemaRoot();
        IntVector y = (IntVector) root.getVector("y");
        assertEquals(3, y.getValueCount());
        assertEquals(101, y.get(0));
        assertEquals(102, y.get(1));
        assertEquals(103, y.get(2));
      }
    }
  }

  /** UDF that ignores its input and returns a constant Scalar. */
  static final class IgnoreInputReturnFortyTwo extends AbstractScalarFunction {
    IgnoreInputReturnFortyTwo() {
      super("forty_two", List.of(INT32), INT32, Volatility.IMMUTABLE);
    }

    @Override
    public ColumnarValue evaluate(BufferAllocator allocator, ScalarFunctionArgs args) {
      IntVector out = new IntVector("out", allocator);
      out.allocateNew(1);
      out.set(0, 42);
      out.setValueCount(1);
      return ColumnarValue.scalar(out);
    }
  }

  @Test
  void udfReturningScalar_isBroadcastByFramework() throws Exception {
    try (SessionContext ctx = new SessionContext();
        BufferAllocator allocator = new RootAllocator()) {
      ctx.registerUdf(new ScalarUdf(new IgnoreInputReturnFortyTwo()));

      try (DataFrame df =
              ctx.sql(
                  "SELECT forty_two(x) AS y"
                      + " FROM (VALUES (CAST(1 AS INT)), (CAST(2 AS INT)),"
                      + " (CAST(3 AS INT)), (CAST(4 AS INT)), (CAST(5 AS INT))) AS t(x)");
          ArrowReader r = df.collect(allocator)) {
        assertEquals(true, r.loadNextBatch());
        VectorSchemaRoot root = r.getVectorSchemaRoot();
        IntVector y = (IntVector) root.getVector("y");
        assertEquals(5, y.getValueCount());
        for (int i = 0; i < 5; i++) {
          assertEquals(42, y.get(i));
        }
      }
    }
  }

  @Test
  void volatilityBytesRoundTrip_forAllThreeKinds() throws Exception {
    for (Volatility v : Volatility.values()) {
      try (SessionContext ctx = new SessionContext();
          BufferAllocator allocator = new RootAllocator()) {
        String registeredName = "add_one_" + v.name().toLowerCase();
        ctx.registerUdf(new ScalarUdf(new AddOne(registeredName, v)));
        try (DataFrame df = ctx.sql("SELECT " + registeredName + "(CAST(0 AS INT))");
            ArrowReader r = df.collect(allocator)) {
          assertEquals(true, r.loadNextBatch());
          IntVector y = (IntVector) r.getVectorSchemaRoot().getVector(0);
          assertEquals(1, y.get(0));
        }
      }
    }
  }
}
