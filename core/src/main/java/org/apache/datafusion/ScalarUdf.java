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

import java.util.List;
import java.util.Objects;

import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * A scalar UDF registration handle: pairs a {@link ScalarFunction} implementation with the metadata
 * DataFusion needs to dispatch SQL calls to it.
 *
 * <p>Mirrors DataFusion's {@code ScalarUDF} struct. Construct one from a {@link ScalarFunction} via
 * {@link #fromImpl(ScalarFunction)} (or the public constructor) and pass it to {@link
 * SessionContext#registerUdf(ScalarUdf)}.
 */
public final class ScalarUdf {
  private final ScalarFunction impl;
  private final String name;
  private final List<ArrowType> argTypes;
  private final ArrowType returnType;
  private final Volatility volatility;

  /**
   * Wrap a {@link ScalarFunction} for registration. Each metadata getter is invoked once here and
   * cached, so a user impl that allocates per call won't be double-evaluated at registration.
   *
   * @throws NullPointerException if {@code impl} or any of its declared metadata is null
   */
  public ScalarUdf(ScalarFunction impl) {
    this.impl = Objects.requireNonNull(impl, "impl");
    this.name = Objects.requireNonNull(impl.name(), "impl.name()");
    this.argTypes = Objects.requireNonNull(impl.argTypes(), "impl.argTypes()");
    this.returnType = Objects.requireNonNull(impl.returnType(), "impl.returnType()");
    this.volatility = Objects.requireNonNull(impl.volatility(), "impl.volatility()");
  }

  /** Equivalent to {@code new ScalarUdf(impl)}; mirrors Rust's {@code ScalarUDF::new_from_impl}. */
  public static ScalarUdf fromImpl(ScalarFunction impl) {
    return new ScalarUdf(impl);
  }

  /** The wrapped implementation. */
  public ScalarFunction impl() {
    return impl;
  }

  /** SQL name; cached from {@link ScalarFunction#name()}. */
  public String name() {
    return name;
  }

  /** Declared argument types; cached from {@link ScalarFunction#argTypes()}. */
  public List<ArrowType> argTypes() {
    return argTypes;
  }

  /** Declared return type; cached from {@link ScalarFunction#returnType()}. */
  public ArrowType returnType() {
    return returnType;
  }

  /** Volatility classification; cached from {@link ScalarFunction#volatility()}. */
  public Volatility volatility() {
    return volatility;
  }
}
