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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

class DataFusionExceptionTest {

  @Test
  void parentExtendsRuntimeException() {
    DataFusionException e = new DataFusionException("x");
    assertInstanceOf(RuntimeException.class, e);
    assertEquals("x", e.getMessage());
  }

  @Test
  void parentCauseConstructorPropagatesCause() {
    Throwable cause = new IllegalStateException("inner");
    DataFusionException e = new DataFusionException("outer", cause);
    assertEquals("outer", e.getMessage());
    assertSame(cause, e.getCause());
  }

  @Test
  void allSubclassesExtendDataFusionException() {
    // Each subclass must extend the parent so callers can `catch
    // (DataFusionException)` for "anything from DataFusion" and `catch
    // (RuntimeException)` keeps working unchanged.
    assertInstanceOf(DataFusionException.class, new PlanException("x"));
    assertInstanceOf(DataFusionException.class, new ExecutionException("x"));
    assertInstanceOf(DataFusionException.class, new ResourcesExhaustedException("x"));
    assertInstanceOf(DataFusionException.class, new IoException("x"));
    assertInstanceOf(DataFusionException.class, new NotImplementedException("x"));
    assertInstanceOf(DataFusionException.class, new ConfigurationException("x"));
  }

  @Test
  void allSubclassesExtendRuntimeException() {
    // Existing callers use `catch (RuntimeException)`. A typed-exception PR
    // that breaks them is a regression even if the new types are correct.
    assertInstanceOf(RuntimeException.class, new PlanException("x"));
    assertInstanceOf(RuntimeException.class, new ExecutionException("x"));
    assertInstanceOf(RuntimeException.class, new ResourcesExhaustedException("x"));
    assertInstanceOf(RuntimeException.class, new IoException("x"));
    assertInstanceOf(RuntimeException.class, new NotImplementedException("x"));
    assertInstanceOf(RuntimeException.class, new ConfigurationException("x"));
  }

  @Test
  void allSubclassesAcceptCauseConstructor() {
    // The (String, Throwable) constructor is the seam upstream issue #55
    // plugs into for Java-throwable propagation from JVM upcalls. v1 has no
    // production caller for it, but it must exist on every subclass so #55
    // doesn't have to add it later.
    Throwable cause = new IllegalStateException("c");
    assertSame(cause, new PlanException("m", cause).getCause());
    assertSame(cause, new ExecutionException("m", cause).getCause());
    assertSame(cause, new ResourcesExhaustedException("m", cause).getCause());
    assertSame(cause, new IoException("m", cause).getCause());
    assertSame(cause, new NotImplementedException("m", cause).getCause());
    assertSame(cause, new ConfigurationException("m", cause).getCause());
  }
}
