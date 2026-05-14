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
 * Volatility classification for a UDF. Mirrors DataFusion's {@code Volatility} enum.
 *
 * <ul>
 *   <li>{@link #IMMUTABLE} — pure function: same inputs always produce the same output; safe to
 *       constant-fold and common-subexpression-eliminate.
 *   <li>{@link #STABLE} — deterministic within a single query but not across queries (e.g., {@code
 *       now()}).
 *   <li>{@link #VOLATILE} — may return a different value on every call (e.g., {@code random()}).
 * </ul>
 */
public enum Volatility {
  IMMUTABLE((byte) 0),
  STABLE((byte) 1),
  VOLATILE((byte) 2);

  private final byte code;

  Volatility(byte code) {
    this.code = code;
  }

  /** Stable byte code for FFI. */
  public byte code() {
    return code;
  }
}
