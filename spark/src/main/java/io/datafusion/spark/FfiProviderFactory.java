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

package io.datafusion.spark;

import java.util.Map;

/**
 * Bridge interface implemented per domain (Rerun, HDF5, custom Iceberg, etc.). A bridge owns its
 * own proto schema for connection options and a cdylib that produces an {@code FFI_TableProvider}
 * pointer. The connector-core Spark plumbing is generic — it knows only this interface.
 *
 * <p>Lifecycle per Spark task:
 *
 * <ol>
 *   <li>{@link #encodeOptions(Map)} — driver-side, converts the Spark options map into the bridge's
 *       own proto bytes; ships verbatim through {@code DatafusionInputPartition}.
 *   <li>{@link #listPartitions(byte[])} — driver-side, enumerates partition identifiers (e.g. Rerun
 *       segment ids) so each gets its own Spark task.
 *   <li>{@link #createProvider(byte[])} — executor-side, builds the bridge's {@code Arc&lt;dyn
 *       TableProvider&gt;}, wraps it in an {@code FFI_TableProvider}, returns the raw boxed pointer
 *       as a {@code jlong}. The caller owns this pointer and is responsible for handing it to
 *       exactly one consumer (the consumer's {@code Drop} releases it).
 * </ol>
 *
 * <p>Implementations must be no-arg constructable so the Spark connector can instantiate them
 * reflectively via {@link Class#forName(String)} on the executor.
 */
public interface FfiProviderFactory {

  /**
   * Convert Spark's flat option map to the bridge's proto-encoded options. Driver-side only.
   *
   * @throws IllegalArgumentException if required options are missing or invalid
   */
  byte[] encodeOptions(Map<String, String> sparkOptions);

  /**
   * Enumerate partition identifiers for this dataset. One Spark task is created per returned id.
   * Driver-side only.
   */
  String[] listPartitions(byte[] optionsProtoBytes);

  /**
   * Build the underlying {@code Arc<dyn TableProvider>} and wrap it in an {@code
   * FFI_TableProvider}. Returns the raw {@code Box::into_raw} pointer as a {@code jlong}; the
   * caller takes ownership.
   */
  long createProvider(byte[] optionsProtoBytes);
}
