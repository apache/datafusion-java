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

import org.junit.jupiter.api.Test;

class SortExprTest {

  @Test
  void ascDefaultsToNullsLast() {
    SortExpr s = SortExpr.asc("a");
    assertTrue(s.ascending());
    assertFalse(s.nullsFirst());
  }

  @Test
  void descDefaultsToNullsFirst() {
    SortExpr s = SortExpr.desc("a");
    assertFalse(s.ascending());
    assertTrue(s.nullsFirst());
  }

  @Test
  void nullsFirstSetterRoundTrips() {
    SortExpr s = SortExpr.asc("a").nullsFirst(true);
    assertTrue(s.nullsFirst());
    s.nullsFirst(false);
    assertFalse(s.nullsFirst());
  }

  @Test
  void factoryRejectsNullColumn() {
    assertThrows(IllegalArgumentException.class, () -> SortExpr.asc(null));
    assertThrows(IllegalArgumentException.class, () -> SortExpr.desc(null));
  }

  @Test
  void accessorReturnsColumnName() {
    assertEquals("foo", SortExpr.asc("foo").column());
    assertEquals("bar", SortExpr.desc("bar").column());
  }
}
