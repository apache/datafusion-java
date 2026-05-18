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
 * Configuration knobs for {@link DataFrame#unnestColumns(UnnestOptions, String...)}, mirroring
 * DataFusion's {@code UnnestOptions}. Defaults match upstream: {@code preserveNulls = true}.
 *
 * <p>Per-column recursion (DataFusion's {@code recursions}) is intentionally not exposed yet — it
 * needs a richer column-pair representation and is filed separately.
 */
public final class UnnestOptions {

  private boolean preserveNulls = true;

  /**
   * When {@code true} (the default), nulls in the input column are preserved as null rows in the
   * output. When {@code false}, nulls and empty lists are dropped.
   */
  public UnnestOptions preserveNulls(boolean v) {
    this.preserveNulls = v;
    return this;
  }

  /** The current {@code preserveNulls} setting. */
  public boolean preserveNulls() {
    return preserveNulls;
  }
}
