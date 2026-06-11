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

import io.datafusion.spark.NativeLibraryLoader;

/**
 * JNI bindings into the example cdylib at {@code examples/native}. The cdylib produces a small
 * {@code MemTable}-backed {@code FFI_TableProvider} that {@link ExampleFfiProviderFactory} hands to
 * the Spark connector ({@code FfiHelperNative.createScan}).
 *
 * <p>The cdylib is bundled inside this jar at {@code
 * org/apache/datafusion/examples/<os>/<arch>/} (see the antrun execution in {@code
 * examples/pom.xml}) and extracted/loaded once via the connector's {@link NativeLibraryLoader} —
 * the same two-piece recipe (pom copy block + one loader call) a real bridge uses to ship its own
 * cdylib. For local hacking against an unpackaged build, {@code
 * -Dexample.ffi.lib.path=/abs/path/to/libdatafusion_java_ffi_example.dylib} bypasses the bundled
 * copy.
 */
final class FfiTableProviderExampleNative {

  private FfiTableProviderExampleNative() {}

  static {
    String explicit = System.getProperty("example.ffi.lib.path");
    if (explicit != null && !explicit.isEmpty()) {
      System.load(explicit);
    } else {
      NativeLibraryLoader.load(
          FfiTableProviderExampleNative.class,
          "org/apache/datafusion/examples",
          "datafusion_java_ffi_example");
    }
  }

  /**
   * Build a {@code MemTable} on the Rust side, wrap it in an {@code FFI_TableProvider}, and return
   * the raw boxed pointer as a {@code long}. Ownership transfers to the caller; passing the pointer
   * to a consumer such as {@code FfiHelperNative.createScan} discharges it.
   *
   * <p>{@code optionsBytes} is the length-prefixed binary blob produced by {@link
   * ExampleFfiProviderFactory#encodeOptions(java.util.Map)}. An empty or {@code null} array decodes
   * as all defaults ({@code name_prefix="row"}, {@code num_rows=4}, {@code num_batches=1}).
   */
  static native long createMemTableProvider(byte[] optionsBytes);

  /**
   * Drop an FFI_TableProvider pointer that was NEVER handed to a consumer. Call this only on the
   * error path before handover; once {@code FfiHelperNative.createScan} (or {@code
   * providerSchemaIpc}) accepts the pointer it owns the box.
   */
  static native void dropProvider(long ffiTableProviderPtr);
}
