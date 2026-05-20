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
 * Join algorithm requested for {@link DataFrame#join} / {@link DataFrame#joinOn}. Mirrors
 * DataFusion's {@code JoinType} enum one-to-one.
 *
 * <ul>
 *   <li>{@link #INNER} — rows where the join condition matches in both sides.
 *   <li>{@link #LEFT} / {@link #RIGHT} — outer joins; unmatched rows on the named side are kept and
 *       padded with nulls on the other side.
 *   <li>{@link #FULL} — full outer join; unmatched rows from either side are kept with nulls.
 *   <li>{@link #LEFT_SEMI} / {@link #RIGHT_SEMI} — returns rows from the named side that have at
 *       least one match on the other; only the named side's columns appear in the output.
 *   <li>{@link #LEFT_ANTI} / {@link #RIGHT_ANTI} — returns rows from the named side that have no
 *       match on the other; only the named side's columns appear in the output.
 *   <li>{@link #LEFT_MARK} / {@link #RIGHT_MARK} — returns one row per row of the named side, with
 *       an additional boolean {@code mark} column indicating whether the join condition matched.
 * </ul>
 */
public enum JoinType {
  INNER((byte) 0),
  LEFT((byte) 1),
  RIGHT((byte) 2),
  FULL((byte) 3),
  LEFT_SEMI((byte) 4),
  RIGHT_SEMI((byte) 5),
  LEFT_ANTI((byte) 6),
  RIGHT_ANTI((byte) 7),
  LEFT_MARK((byte) 8),
  RIGHT_MARK((byte) 9);

  private final byte code;

  JoinType(byte code) {
    this.code = code;
  }

  /** Stable byte code for FFI. */
  public byte code() {
    return code;
  }
}
