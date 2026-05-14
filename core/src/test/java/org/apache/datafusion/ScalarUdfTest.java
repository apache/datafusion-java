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
}
