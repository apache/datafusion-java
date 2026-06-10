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

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.datafusion.DataFrame;
import org.apache.datafusion.SessionContext;

/**
 * Register a Rust-built {@code FFI_TableProvider} on a {@link SessionContext} and run SQL against
 * it.
 *
 * <p>The provider here wraps a tiny in-memory table (4 rows, 3 columns) built by the example cdylib
 * under {@code examples/native}. The same {@link SessionContext#registerFfiTable(String, long)}
 * entry point is what domain bridges (Rerun, HDF5, custom Iceberg) use to expose their native
 * {@code TableProvider}s — and, transitively, what the Spark connector uses through the {@code
 * FfiProviderFactory} interface in {@code connector-core} (see {@code
 * examples/SPARK_INTEGRATION.md}).
 *
 * <p>How to run (from the fork repo root):
 *
 * <pre>{@code
 * cargo build -p datafusion-java-ffi-example --release
 * mvn -B install -DskipTests -Drat.skip=true \
 *     -Ddatafusion.native.profile=release
 * mvn -B -pl examples exec:exec \
 *     -Dexec.mainClass=org.apache.datafusion.examples.FfiTableProviderExample
 * }</pre>
 *
 * <p>The first {@code mvn install} step publishes {@code datafusion-java} to your local Maven repo
 * so the separate {@code exec:exec} invocation can resolve it as a dependency. Skipping straight to
 * {@code exec:exec} after a {@code package} build fails with {@code Could not find artifact
 * org.apache.datafusion:datafusion-java:...}.
 */
public final class FfiTableProviderExample {

  private FfiTableProviderExample() {}

  public static void main(String[] args) throws Exception {
    // Build the FFI provider on the Rust side. The returned `long` is a
    // `Box::into_raw(Box::new(FFI_TableProvider))` pointer; ownership flows
    // through `registerFfiTable` into the SessionContext.
    long ffiProviderPtr = FfiTableProviderExampleNative.createMemTableProvider();
    if (ffiProviderPtr == 0) {
      throw new IllegalStateException("Native FFI provider builder returned 0");
    }

    try (var allocator = new RootAllocator();
        var ctx = new SessionContext()) {

      // Hand the raw pointer to DataFusion. After this call, the SessionContext
      // owns the boxed FFI_TableProvider; do NOT call dropProvider afterwards.
      ctx.registerFfiTable("example_mem", ffiProviderPtr);

      // Filter pushdown crosses the FFI boundary transparently — DataFusion's
      // optimizer rewrites the predicate into a TableProviderFilterPushDown
      // call on the foreign provider, which a MemTable handles unsupported
      // (the executor re-applies it above the scan).
      try (DataFrame df =
              ctx.sql("SELECT id, name, value FROM example_mem WHERE id > 1 ORDER BY id");
          ArrowReader reader = df.collect(allocator)) {
        System.out.println("Result rows:");
        while (reader.loadNextBatch()) {
          VectorSchemaRoot batch = reader.getVectorSchemaRoot();
          System.out.print(batch.contentToTSVString());
        }
      }
    }
  }
}
