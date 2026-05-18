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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

class ColumnarValueTest {

  private static final ArrowType INT32 = new ArrowType.Int(32, true);

  @Test
  void array_factory_returnsArrayVariant() {
    try (BufferAllocator allocator = new RootAllocator();
        IntVector v = new IntVector("v", allocator)) {
      v.allocateNew(3);
      v.setValueCount(3);
      ColumnarValue cv = ColumnarValue.array(v);
      assertSame(v, cv.vector());
      assertEquals(INT32, cv.dataType());
    }
  }

  @Test
  void scalar_factory_returnsScalarVariant() {
    try (BufferAllocator allocator = new RootAllocator();
        IntVector v = new IntVector("v", allocator)) {
      v.allocateNew(1);
      v.set(0, 42);
      v.setValueCount(1);
      ColumnarValue cv = ColumnarValue.scalar(v);
      assertSame(v, cv.vector());
      assertEquals(INT32, cv.dataType());
    }
  }

  @Test
  void scalar_factory_rejectsNonOneLength() {
    try (BufferAllocator allocator = new RootAllocator();
        IntVector v = new IntVector("v", allocator)) {
      v.allocateNew(2);
      v.setValueCount(2);
      IllegalArgumentException ex =
          assertThrows(IllegalArgumentException.class, () -> ColumnarValue.scalar(v));
      assertEquals("Scalar vector must have valueCount == 1, got 2", ex.getMessage());
    }
  }

  @Test
  void array_factory_rejectsNull() {
    assertThrows(NullPointerException.class, () -> ColumnarValue.array(null));
  }

  @Test
  void scalar_factory_rejectsNull() {
    assertThrows(NullPointerException.class, () -> ColumnarValue.scalar(null));
  }
}
