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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.junit.jupiter.api.Test;

class ScalarFunctionArgsTest {

  @Test
  void construct_emptyArgs_zeroRows_ok() {
    ScalarFunctionArgs a = new ScalarFunctionArgs(List.of(), 0);
    assertEquals(List.of(), a.args());
    assertEquals(0, a.rowCount());
  }

  @Test
  void construct_rejectsNullArgs() {
    assertThrows(NullPointerException.class, () -> new ScalarFunctionArgs(null, 3));
  }

  @Test
  void construct_rejectsNegativeRowCount() {
    assertThrows(IllegalArgumentException.class, () -> new ScalarFunctionArgs(List.of(), -1));
  }

  @Test
  void construct_copiesArgsDefensively() {
    try (BufferAllocator allocator = new RootAllocator();
        IntVector v = new IntVector("v", allocator)) {
      v.allocateNew(1);
      v.setValueCount(1);
      List<ColumnarValue> source = new ArrayList<>();
      source.add(ColumnarValue.scalar(v));
      ScalarFunctionArgs a = new ScalarFunctionArgs(source, 1);
      source.clear();
      assertEquals(1, a.args().size());
      assertThrows(UnsupportedOperationException.class, () -> a.args().clear());
    }
  }

  @Test
  void args_singletonCase_preservesValue() {
    try (BufferAllocator allocator = new RootAllocator();
        IntVector v = new IntVector("v", allocator)) {
      v.allocateNew(1);
      v.set(0, 7);
      v.setValueCount(1);
      ScalarFunctionArgs a =
          new ScalarFunctionArgs(Collections.singletonList(ColumnarValue.scalar(v)), 5);
      assertEquals(5, a.rowCount());
      assertTrue(a.args().get(0) instanceof ColumnarValue.Scalar);
    }
  }
}
