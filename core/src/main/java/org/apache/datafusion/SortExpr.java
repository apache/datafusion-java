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

/**
 * A single sort key passed to {@link DataFrame#sort(SortExpr...)}, mirroring DataFusion's {@code
 * expr::Sort{ expr, asc, nulls_first }}. Build via {@link #asc(String)} or {@link #desc(String)};
 * tweak null placement via {@link #nullsFirst(boolean)}.
 *
 * <p>v1 accepts column names only -- the {@code column} string is interpreted as a column
 * reference, not a SQL expression. Complex sort keys (e.g. {@code "a + b"}) are intentionally
 * deferred until the Java binding gains an {@code Expr} builder.
 *
 * <p>Defaults match DataFusion: {@link #asc(String)} sorts ascending with NULLs last; {@link
 * #desc(String)} sorts descending with NULLs first.
 */
public final class SortExpr {

  private final String column;
  private final boolean ascending;
  private boolean nullsFirst;

  private SortExpr(String column, boolean ascending, boolean nullsFirst) {
    this.column = column;
    this.ascending = ascending;
    this.nullsFirst = nullsFirst;
  }

  /** Sort the given column ascending, with NULLs placed last. */
  public static SortExpr asc(String column) {
    if (column == null) {
      throw new IllegalArgumentException("SortExpr column must be non-null");
    }
    return new SortExpr(column, true, false);
  }

  /** Sort the given column descending, with NULLs placed first. */
  public static SortExpr desc(String column) {
    if (column == null) {
      throw new IllegalArgumentException("SortExpr column must be non-null");
    }
    return new SortExpr(column, false, true);
  }

  /** Override the default NULL placement. {@code true} = NULLs first, {@code false} = last. */
  public SortExpr nullsFirst(boolean v) {
    this.nullsFirst = v;
    return this;
  }

  /** The column name this sort key references. */
  public String column() {
    return column;
  }

  /** {@code true} if ascending, {@code false} if descending. */
  public boolean ascending() {
    return ascending;
  }

  /** {@code true} if NULLs are placed first, {@code false} if last. */
  public boolean nullsFirst() {
    return nullsFirst;
  }
}
