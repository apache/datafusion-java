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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

/**
 * JNI bindings into the example cdylib at {@code examples/native}. The cdylib produces a small
 * {@code MemTable}-backed {@code FFI_TableProvider} that the JVM example registers on a {@link
 * org.apache.datafusion.SessionContext} via {@link
 * org.apache.datafusion.SessionContext#registerFfiTable(String, long)}.
 *
 * <p>The library is located in this order:
 *
 * <ol>
 *   <li>Absolute path passed via {@code -Dexample.ffi.lib.path=/abs/path/to/lib...}.
 *   <li>{@code rust-target/release/<mappedName>} relative to the current working directory
 *       (the workspace output dir; default when invoked via {@code mvn exec:java} from the
 *       repo root).
 *   <li>{@code rust-target/debug/<mappedName>} as a fallback for {@code cargo build} without
 *       {@code --release}.
 * </ol>
 *
 * <p>If none of these exist, an {@link UnsatisfiedLinkError} surfaces with the search list so the
 * user knows what to build.
 */
final class FfiTableProviderExampleNative {

  private static final String LIBRARY_NAME = "datafusion_java_ffi_example";

  private FfiTableProviderExampleNative() {}

  static {
    loadLibrary();
  }

  /**
   * Build a {@code MemTable} on the Rust side, wrap it in an {@code FFI_TableProvider}, and return
   * the raw boxed pointer as a {@code long}. Ownership transfers to the caller; passing the pointer
   * to {@link org.apache.datafusion.SessionContext#registerFfiTable(String, long)} discharges it.
   *
   * <p>{@code optionsBytes} is the length-prefixed binary blob produced by {@link
   * ExampleFfiProviderFactory#encodeOptions(java.util.Map)}. An empty or {@code null} array
   * decodes as all defaults ({@code name_prefix="row"}, {@code num_rows=4}, {@code
   * num_batches=1}).
   */
  static native long createMemTableProvider(byte[] optionsBytes);

  /**
   * Drop an FFI_TableProvider pointer that was NEVER handed to {@code
   * SessionContext.registerFfiTable}. Call this only on the error path before registration; once
   * {@code registerFfiTable} accepts the pointer it owns the box.
   */
  static native void dropProvider(long ffiTableProviderPtr);

  private static void loadLibrary() {
    String mapped = System.mapLibraryName(LIBRARY_NAME);
    Path explicit = optionalPath(System.getProperty("example.ffi.lib.path"));

    // Cover both common cwds: repo root (mvn exec from datafusion-java/) and
    // the examples module (mvn exec from datafusion-java/examples/). The
    // workspace writes to `rust-target/` at the repo root.
    Path[] candidates =
        new Path[] {
          explicit,
          Paths.get("rust-target", "release", mapped),
          Paths.get("rust-target", "debug", mapped),
          Paths.get("..", "rust-target", "release", mapped),
          Paths.get("..", "rust-target", "debug", mapped),
        };

    for (Path candidate : candidates) {
      if (candidate != null && Files.exists(candidate)) {
        System.load(candidate.toAbsolutePath().toString());
        return;
      }
    }

    StringBuilder searched = new StringBuilder();
    for (Path c : candidates) {
      if (searched.length() > 0) searched.append(", ");
      searched.append(c == null ? "null" : c.toAbsolutePath().toString());
    }
    throw new UnsatisfiedLinkError(
        String.format(
            Locale.ROOT,
            "Example native library %s not found. Searched: [%s]. "
                + "Build with 'cargo build -p datafusion-java-ffi-example --release', or pass "
                + "-Dexample.ffi.lib.path=<absolute path to the cdylib>.",
            mapped,
            searched));
  }

  private static Path optionalPath(String s) {
    return (s == null || s.isEmpty()) ? null : Paths.get(s);
  }
}
