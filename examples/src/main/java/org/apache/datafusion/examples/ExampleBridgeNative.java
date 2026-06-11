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
 * JNI surface generated on the Rust side by {@code export_bridge!} in {@code
 * examples/native/src/lib.rs} with {@code jni_class =
 * "org_apache_datafusion_examples_ExampleBridgeNative"} — the mangled binary name of THIS class.
 * Renaming or moving this class requires regenerating the Rust macro invocation to match.
 *
 * <p>The cdylib is bundled inside this jar at {@code org/apache/datafusion/examples/<os>/<arch>/}
 * (see the antrun execution in {@code examples/pom.xml}). For local hacking against an unpackaged
 * build, {@code -Dexample.bridge.lib.path=/abs/path/to/libdatafusion_example_bridge.dylib} bypasses
 * the bundled copy.
 */
final class ExampleBridgeNative {

  private ExampleBridgeNative() {}

  static {
    String explicit = System.getProperty("example.bridge.lib.path");
    if (explicit != null && !explicit.isEmpty()) {
      System.load(explicit);
    } else {
      NativeLibraryLoader.load(
          ExampleBridgeNative.class, "org/apache/datafusion/examples", "datafusion_example_bridge");
    }
  }

  static native byte[] providerSchemaIpc(byte[] options, byte[] partition);

  static native long createScan(
      byte[] options,
      byte[] partition,
      int targetPartitions,
      int batchSize,
      String[] optionKeys,
      String[] optionValues,
      String[] projectionColumns,
      byte[][] filterProtos);

  static native int partitionCount(long scanHandle);

  static native void executeStreamPartition(long scanHandle, int partition, long ffiStreamAddr);

  static native void executeStream(long scanHandle, long ffiStreamAddr);

  static native void closeScan(long scanHandle);
}
