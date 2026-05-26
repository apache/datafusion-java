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

package org.apache.datafusion.examples;

import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.datafusion.ColumnarValue;
import org.apache.datafusion.DataFrame;
import org.apache.datafusion.ExceptionVerbosity;
import org.apache.datafusion.ScalarFunction;
import org.apache.datafusion.ScalarFunctionArgs;
import org.apache.datafusion.ScalarUdf;
import org.apache.datafusion.SessionContext;
import org.apache.datafusion.Volatility;

/**
 * Demonstrates the three {@link ExceptionVerbosity} settings on the same throwing UDF. The same
 * {@code BoomUdf} is invoked under each verbosity; the exception message attached to the resulting
 * {@link RuntimeException} differs in detail.
 *
 * <p>Run with:
 *
 * <pre>
 * ./mvnw -pl :datafusion-java-examples exec:exec \
 *     -Dexec.mainClass=org.apache.datafusion.examples.ExceptionVerbosityExample
 * </pre>
 */
public final class ExceptionVerbosityExample {

  /** UDF that always throws. The message and stack frame are what the example surfaces. */
  static final class BoomUdf implements ScalarFunction {
    @Override
    public String name() {
      return "boom";
    }

    @Override
    public List<Field> argFields() {
      return List.of(Field.nullable("x", new ArrowType.Int(32, true)));
    }

    @Override
    public Field returnField() {
      return Field.nullable("y", new ArrowType.Int(32, true));
    }

    @Override
    public Volatility volatility() {
      return Volatility.IMMUTABLE;
    }

    @Override
    public ColumnarValue evaluate(BufferAllocator allocator, ScalarFunctionArgs args) {
      throw new IllegalStateException("user-supplied input failed validation");
    }
  }

  public static void main(String[] args) {
    for (ExceptionVerbosity v : ExceptionVerbosity.values()) {
      runWith(v);
    }
  }

  private static void runWith(ExceptionVerbosity verbosity) {
    System.out.println("=== ExceptionVerbosity." + verbosity + " ===");
    try (SessionContext ctx = SessionContext.builder().exceptionVerbosity(verbosity).build();
        BufferAllocator allocator = new RootAllocator()) {
      ctx.registerUdf(new ScalarUdf(new BoomUdf()));
      try (DataFrame df = ctx.sql("SELECT boom(CAST(1 AS INT))");
          ArrowReader reader = df.collect(allocator)) {
        while (reader.loadNextBatch()) {
          // The UDF throws; we never reach this line.
        }
      } catch (RuntimeException e) {
        System.out.println(e.getMessage());
      } catch (Exception e) {
        // ArrowReader.close() is declared to throw; treat the same.
        System.out.println(e.getMessage());
      }
    }
    System.out.println();
  }

  private ExceptionVerbosityExample() {}
}
