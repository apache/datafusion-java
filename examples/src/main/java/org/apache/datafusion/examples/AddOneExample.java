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
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.datafusion.ColumnarValue;
import org.apache.datafusion.DataFrame;
import org.apache.datafusion.ScalarFunction;
import org.apache.datafusion.ScalarFunctionArgs;
import org.apache.datafusion.ScalarUdf;
import org.apache.datafusion.SessionContext;
import org.apache.datafusion.Volatility;

/** Demonstrates registering a Java scalar UDF and invoking it from SQL. */
public final class AddOneExample {

  /** Adds 1 to each value of an Int32 column. */
  public static final class AddOne implements ScalarFunction {
    private static final ArrowType INT32 = new ArrowType.Int(32, true);

    @Override
    public String name() {
      return "add_one";
    }

    @Override
    public List<Field> argFields() {
      return List.of(Field.nullable("x", INT32));
    }

    @Override
    public Field returnField() {
      return Field.nullable("y", INT32);
    }

    @Override
    public Volatility volatility() {
      return Volatility.IMMUTABLE;
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

  public static void main(String[] args) throws Exception {
    try (SessionContext ctx = new SessionContext();
        BufferAllocator allocator = new RootAllocator()) {
      ctx.registerUdf(new ScalarUdf(new AddOne()));

      try (DataFrame df =
              ctx.sql(
                  "SELECT add_one(x) AS y FROM (VALUES (CAST(1 AS INT)),(CAST(2 AS INT)),(CAST(3 AS"
                      + " INT))) AS t(x)");
          ArrowReader reader = df.collect(allocator)) {
        while (reader.loadNextBatch()) {
          VectorSchemaRoot root = reader.getVectorSchemaRoot();
          IntVector y = (IntVector) root.getVector("y");
          for (int i = 0; i < y.getValueCount(); i++) {
            System.out.println("y = " + y.get(i));
          }
        }
      }
    }
  }
}
