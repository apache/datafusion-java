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

import org.junit.jupiter.api.Test;

class DataFrameJoinTest {

  // Two relations with one matching key (1, 2) and a few unmatched rows on each side.
  // left: (1,'a'), (2,'b'), (3,'c'); right: (1,10), (2,20), (4,40).
  private static final String LEFT_SQL =
      "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS l(id, s)";
  private static final String RIGHT_SQL =
      "SELECT * FROM (VALUES (1, 10), (2, 20), (4, 40)) AS r(id, v)";

  @Test
  void innerJoinOnSingleColumn() {
    try (SessionContext ctx = new SessionContext();
        DataFrame left = ctx.sql(LEFT_SQL);
        DataFrame right = ctx.sql(RIGHT_SQL);
        DataFrame joined =
            left.join(right, JoinType.INNER, new String[] {"id"}, new String[] {"id"})) {
      assertEquals(2L, joined.count()); // (1,'a',1,10) and (2,'b',2,20)
    }
  }

  @Test
  void innerJoinOnMultipleColumns() {
    try (SessionContext ctx = new SessionContext();
        DataFrame l =
            ctx.sql("SELECT * FROM (VALUES (1, 'x', 100), (2, 'y', 200)) AS t(a, b, l_other)");
        DataFrame r =
            ctx.sql("SELECT * FROM (VALUES (1, 'x', 'p'), (2, 'z', 'q')) AS t(a2, b2, r_other)");
        DataFrame joined =
            l.join(r, JoinType.INNER, new String[] {"a", "b"}, new String[] {"a2", "b2"})) {
      assertEquals(1L, joined.count()); // only (1,'x') matches on both keys
    }
  }

  @Test
  void leftJoinPreservesUnmatchedLeft() {
    try (SessionContext ctx = new SessionContext();
        DataFrame left = ctx.sql(LEFT_SQL);
        DataFrame right = ctx.sql(RIGHT_SQL);
        DataFrame joined =
            left.join(right, JoinType.LEFT, new String[] {"id"}, new String[] {"id"})) {
      // 3 left rows; unmatched (3,'c') gets nulls on the right side.
      assertEquals(3L, joined.count());
    }
  }

  @Test
  void rightJoinPreservesUnmatchedRight() {
    try (SessionContext ctx = new SessionContext();
        DataFrame left = ctx.sql(LEFT_SQL);
        DataFrame right = ctx.sql(RIGHT_SQL);
        DataFrame joined =
            left.join(right, JoinType.RIGHT, new String[] {"id"}, new String[] {"id"})) {
      // 3 right rows; unmatched (4,40) gets nulls on the left side.
      assertEquals(3L, joined.count());
    }
  }

  @Test
  void fullJoinPreservesBothSides() {
    try (SessionContext ctx = new SessionContext();
        DataFrame left = ctx.sql(LEFT_SQL);
        DataFrame right = ctx.sql(RIGHT_SQL);
        DataFrame joined =
            left.join(right, JoinType.FULL, new String[] {"id"}, new String[] {"id"})) {
      // 2 matched rows + 1 unmatched-left + 1 unmatched-right = 4.
      assertEquals(4L, joined.count());
    }
  }

  @Test
  void leftSemiJoinReturnsLeftMatchedOnly() {
    try (SessionContext ctx = new SessionContext();
        DataFrame left = ctx.sql(LEFT_SQL);
        DataFrame right = ctx.sql(RIGHT_SQL);
        DataFrame joined =
            left.join(right, JoinType.LEFT_SEMI, new String[] {"id"}, new String[] {"id"})) {
      // Only the 2 left rows that have a matching right row.
      // Output projects left side only (id, s) — right columns dropped.
      assertEquals(2L, joined.count());
    }
  }

  @Test
  void leftAntiJoinReturnsLeftUnmatchedOnly() {
    try (SessionContext ctx = new SessionContext();
        DataFrame left = ctx.sql(LEFT_SQL);
        DataFrame right = ctx.sql(RIGHT_SQL);
        DataFrame joined =
            left.join(right, JoinType.LEFT_ANTI, new String[] {"id"}, new String[] {"id"})) {
      // Only the 1 left row (3,'c') with no right match. Output projects left side only.
      assertEquals(1L, joined.count());
    }
  }

  @Test
  void rightSemiJoinReturnsRightMatchedOnly() {
    try (SessionContext ctx = new SessionContext();
        DataFrame left = ctx.sql(LEFT_SQL);
        DataFrame right = ctx.sql(RIGHT_SQL);
        DataFrame joined =
            left.join(right, JoinType.RIGHT_SEMI, new String[] {"id"}, new String[] {"id"})) {
      // Output projects right side only (id, v) — left columns dropped.
      assertEquals(2L, joined.count());
    }
  }

  @Test
  void rightAntiJoinReturnsRightUnmatchedOnly() {
    try (SessionContext ctx = new SessionContext();
        DataFrame left = ctx.sql(LEFT_SQL);
        DataFrame right = ctx.sql(RIGHT_SQL);
        DataFrame joined =
            left.join(right, JoinType.RIGHT_ANTI, new String[] {"id"}, new String[] {"id"})) {
      assertEquals(1L, joined.count()); // (4, 40)
    }
  }

  @Test
  void leftMarkJoinAddsMarkColumn() {
    try (SessionContext ctx = new SessionContext();
        DataFrame left = ctx.sql(LEFT_SQL);
        DataFrame right = ctx.sql(RIGHT_SQL);
        DataFrame joined =
            left.join(right, JoinType.LEFT_MARK, new String[] {"id"}, new String[] {"id"})) {
      // One row per left row, plus a 'mark' boolean column.
      assertEquals(3L, joined.count());
    }
  }

  @Test
  void joinWithResidualFilter() {
    try (SessionContext ctx = new SessionContext();
        DataFrame left = ctx.sql(LEFT_SQL);
        DataFrame right = ctx.sql(RIGHT_SQL);
        DataFrame joined =
            left.join(right, JoinType.INNER, new String[] {"id"}, new String[] {"id"}, "v >= 20")) {
      // Without the filter: 2 matched rows. With v >= 20: only (2,'b',2,20).
      assertEquals(1L, joined.count());
    }
  }

  @Test
  void joinOnSingleEqualityPredicate() {
    try (SessionContext ctx = new SessionContext();
        DataFrame left = ctx.sql(LEFT_SQL);
        DataFrame right = ctx.sql(RIGHT_SQL);
        DataFrame joined = left.joinOn(right, JoinType.INNER, "l.id = r.id")) {
      assertEquals(2L, joined.count());
    }
  }

  @Test
  void joinOnInequalityPredicate() {
    try (SessionContext ctx = new SessionContext();
        DataFrame left = ctx.sql(LEFT_SQL);
        DataFrame right = ctx.sql(RIGHT_SQL);
        DataFrame joined = left.joinOn(right, JoinType.INNER, "l.id < r.id")) {
      // Pairs (1,'a')<(2,20), (1,'a')<(4,40), (2,'b')<(4,40), (3,'c')<(4,40) = 4.
      assertEquals(4L, joined.count());
    }
  }

  @Test
  void joinOnMultiplePredicates() {
    try (SessionContext ctx = new SessionContext();
        DataFrame left = ctx.sql(LEFT_SQL);
        DataFrame right = ctx.sql(RIGHT_SQL);
        DataFrame joined = left.joinOn(right, JoinType.INNER, "l.id = r.id", "r.v > 15")) {
      // Equality narrows to (1,'a',1,10) and (2,'b',2,20); v > 15 leaves only the second.
      assertEquals(1L, joined.count());
    }
  }

  @Test
  void semiJoinWithFilterToleratesSharedUnqualifiedColumn() {
    // Regression for issue surfaced in code review: when both inputs carry an unqualified
    // column with the same name (here, `tag`) that the residual filter does NOT reference,
    // the join must still plan. Earlier the Rust side merged the schemas via
    // DFSchema::join, whose check_names rejected the duplicate before parsing the filter.
    try (SessionContext ctx = new SessionContext();
        DataFrame left = ctx.sql("SELECT 1 AS id, 'l' AS tag");
        DataFrame right = ctx.sql("SELECT 1 AS rid, 99 AS rv, 'r' AS tag");
        DataFrame joined =
            left.join(
                right, JoinType.LEFT_SEMI, new String[] {"id"}, new String[] {"rid"}, "rv > 0")) {
      assertEquals(1L, joined.count());
    }
  }

  @Test
  void joinOnToleratesSharedUnqualifiedColumn() {
    // Same regression as the previous test, but exercises the joinOn predicate path.
    // Uses LEFT_SEMI so the output schema is one-sided -- INNER joins on inputs that share
    // an unqualified column name are genuinely ambiguous in the result and rejected by
    // upstream's build_join_schema, which is not specific to our code.
    try (SessionContext ctx = new SessionContext();
        DataFrame left = ctx.sql("SELECT 1 AS id, 'l' AS tag");
        DataFrame right = ctx.sql("SELECT 1 AS rid, 99 AS rv, 'r' AS tag");
        DataFrame joined = left.joinOn(right, JoinType.LEFT_SEMI, "id = rid", "rv > 0")) {
      assertEquals(1L, joined.count());
    }
  }

  @Test
  void joinPreservesReceivers() {
    try (SessionContext ctx = new SessionContext();
        DataFrame left = ctx.sql(LEFT_SQL);
        DataFrame right = ctx.sql(RIGHT_SQL)) {
      try (DataFrame joined =
          left.join(right, JoinType.INNER, new String[] {"id"}, new String[] {"id"})) {
        assertEquals(2L, joined.count());
      }
      // Both receivers still usable after join().
      assertEquals(3L, left.count());
      assertEquals(3L, right.count());
    }
  }

  @Test
  void joinThrowsWhenLeftClosed() {
    try (SessionContext ctx = new SessionContext();
        DataFrame right = ctx.sql(RIGHT_SQL)) {
      DataFrame left = ctx.sql(LEFT_SQL);
      left.close();
      assertThrows(
          IllegalStateException.class,
          () -> left.join(right, JoinType.INNER, new String[] {"id"}, new String[] {"id"}));
    }
  }

  @Test
  void joinThrowsWhenRightClosed() {
    try (SessionContext ctx = new SessionContext();
        DataFrame left = ctx.sql(LEFT_SQL)) {
      DataFrame right = ctx.sql(RIGHT_SQL);
      right.close();
      assertThrows(
          IllegalStateException.class,
          () -> left.join(right, JoinType.INNER, new String[] {"id"}, new String[] {"id"}));
    }
  }

  @Test
  void joinNullArgumentValidation() {
    try (SessionContext ctx = new SessionContext();
        DataFrame left = ctx.sql(LEFT_SQL);
        DataFrame right = ctx.sql(RIGHT_SQL)) {
      String[] cols = new String[] {"id"};
      assertThrows(
          IllegalArgumentException.class, () -> left.join(null, JoinType.INNER, cols, cols));
      assertThrows(IllegalArgumentException.class, () -> left.join(right, null, cols, cols));
      assertThrows(
          IllegalArgumentException.class, () -> left.join(right, JoinType.INNER, null, cols));
      assertThrows(
          IllegalArgumentException.class, () -> left.join(right, JoinType.INNER, cols, null));
      assertThrows(
          IllegalArgumentException.class,
          () -> left.join(right, JoinType.INNER, new String[] {"id", "id"}, cols));
      assertThrows(
          IllegalArgumentException.class, () -> left.join(right, JoinType.INNER, cols, cols, null));
      assertThrows(IllegalArgumentException.class, () -> left.joinOn(null, JoinType.INNER, "1=1"));
      assertThrows(IllegalArgumentException.class, () -> left.joinOn(right, null, "1=1"));
      assertThrows(IllegalArgumentException.class, () -> left.joinOn(right, JoinType.INNER));
      assertThrows(
          IllegalArgumentException.class, () -> left.joinOn(right, JoinType.INNER, (String) null));
    }
  }
}
