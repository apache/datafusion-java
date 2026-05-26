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

import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.datafusion.ColumnarValue;
import org.apache.datafusion.DataFrame;
import org.apache.datafusion.ScalarFunction;
import org.apache.datafusion.ScalarFunctionArgs;
import org.apache.datafusion.ScalarUdf;
import org.apache.datafusion.SessionContext;
import org.apache.datafusion.Volatility;

/**
 * Demonstrates a scalar UDF over a nested Arrow type.
 *
 * <p>The function takes a {@code List<Int32>} and returns the sum of its elements as Int64. The
 * argument is declared with a {@link Field} carrying a child element field, which is how nested
 * Arrow types — List, Struct, Map, Union — express their inner type information.
 */
public final class NestedTypeUdfExample {

  /** Sums the Int32 elements of a List<Int32> column. Null lists yield null; null elements skip. */
  public static final class ListSum implements ScalarFunction {
    private static final ArrowType INT32 = new ArrowType.Int(32, true);
    private static final ArrowType INT64 = new ArrowType.Int(64, true);

    @Override
    public String name() {
      return "java_list_sum";
    }

    @Override
    public List<Field> argFields() {
      // List<Int32> needs the element type as a child Field; ArrowType.List alone does not carry
      // it.
      return List.of(
          new Field(
              "vals",
              FieldType.nullable(new ArrowType.List()),
              List.of(Field.nullable("item", INT32))));
    }

    @Override
    public Field returnField() {
      return Field.nullable("s", INT64);
    }

    @Override
    public Volatility volatility() {
      return Volatility.IMMUTABLE;
    }

    @Override
    public ColumnarValue evaluate(BufferAllocator allocator, ScalarFunctionArgs args) {
      ListVector in = (ListVector) args.args().get(0).vector();
      IntVector elems = (IntVector) in.getDataVector();
      BigIntVector out = new BigIntVector("list_sum_out", allocator);
      int n = in.getValueCount();
      out.allocateNew(n);
      for (int i = 0; i < n; i++) {
        if (in.isNull(i)) {
          out.setNull(i);
          continue;
        }
        long sum = 0L;
        int start = in.getElementStartIndex(i);
        int end = in.getElementEndIndex(i);
        for (int j = start; j < end; j++) {
          if (!elems.isNull(j)) {
            sum += elems.get(j);
          }
        }
        out.set(i, sum);
      }
      out.setValueCount(n);
      return ColumnarValue.array(out);
    }
  }

  public static void main(String[] args) throws Exception {
    try (SessionContext ctx = new SessionContext();
        BufferAllocator allocator = new RootAllocator()) {
      ctx.registerUdf(new ScalarUdf(new ListSum()));

      try (DataFrame df =
              ctx.sql(
                  "SELECT java_list_sum(vals) AS s"
                      + " FROM (VALUES"
                      + "   (make_array(CAST(1 AS INT), CAST(2 AS INT), CAST(3 AS INT))),"
                      + "   (make_array(CAST(10 AS INT), CAST(20 AS INT))),"
                      + "   (make_array(CAST(7 AS INT)))"
                      + " ) AS t(vals)");
          ArrowReader reader = df.collect(allocator)) {
        while (reader.loadNextBatch()) {
          VectorSchemaRoot root = reader.getVectorSchemaRoot();
          BigIntVector s = (BigIntVector) root.getVector("s");
          for (int i = 0; i < s.getValueCount(); i++) {
            System.out.println("sum = " + s.get(i));
          }
        }
      }
    }
  }
}
