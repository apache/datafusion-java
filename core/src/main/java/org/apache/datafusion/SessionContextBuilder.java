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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.datafusion.protobuf.ConfigOption;
import org.apache.datafusion.protobuf.MemoryLimit;
import org.apache.datafusion.protobuf.SessionOptions;

/**
 * Builder for a configured {@link SessionContext}. Each setter is optional; unset fields leave the
 * DataFusion default in place.
 */
public final class SessionContextBuilder {
  private Integer batchSize;
  private Integer targetPartitions;
  private Boolean collectStatistics;
  private Boolean informationSchema;
  private Long memoryLimitBytes;
  private Double memoryLimitFraction;
  private String tempDirectory;
  private final LinkedHashMap<String, String> options = new LinkedHashMap<>();

  SessionContextBuilder() {}

  public SessionContextBuilder batchSize(int batchSize) {
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be positive, got " + batchSize);
    }
    this.batchSize = batchSize;
    return this;
  }

  public SessionContextBuilder targetPartitions(int targetPartitions) {
    if (targetPartitions <= 0) {
      throw new IllegalArgumentException(
          "targetPartitions must be positive, got " + targetPartitions);
    }
    this.targetPartitions = targetPartitions;
    return this;
  }

  public SessionContextBuilder collectStatistics(boolean collectStatistics) {
    this.collectStatistics = collectStatistics;
    return this;
  }

  public SessionContextBuilder informationSchema(boolean informationSchema) {
    this.informationSchema = informationSchema;
    return this;
  }

  /**
   * Cap the memory pool at {@code maxMemoryBytes}, reserving {@code fraction} of it for queries.
   */
  public SessionContextBuilder memoryLimit(long maxMemoryBytes, double fraction) {
    if (maxMemoryBytes <= 0) {
      throw new IllegalArgumentException("maxMemoryBytes must be positive, got " + maxMemoryBytes);
    }
    if (fraction <= 0.0 || fraction > 1.0) {
      throw new IllegalArgumentException("fraction must be in (0, 1], got " + fraction);
    }
    this.memoryLimitBytes = maxMemoryBytes;
    this.memoryLimitFraction = fraction;
    return this;
  }

  /** Directory the DiskManager uses for spill files. */
  public SessionContextBuilder tempDirectory(String path) {
    this.tempDirectory = path;
    return this;
  }

  /**
   * Set an arbitrary {@code datafusion.*} config option by string key. Mirrors DataFusion's {@code
   * ConfigOptions::set(key, value)} API — see the DataFusion configuration reference for the full
   * set of keys. Entries set this way are applied <strong>after</strong> the typed setters on this
   * builder, so an explicit {@code setOption} call overrides a typed setter for the same knob.
   *
   * <p>{@code datafusion.runtime.*} keys (memory limit, temp directory, cache sizes, etc) are not
   * yet supported by this setter and will throw at {@link #build()}. Use the typed {@link
   * #memoryLimit(long, double)} and {@link #tempDirectory(String)} setters instead. Round-trip
   * support for the runtime subtree is tracked as a follow-up.
   *
   * <p>Unknown keys or unparseable values are not validated here; they surface as a {@link
   * RuntimeException} from {@link #build()} carrying DataFusion's error message.
   *
   * @throws IllegalArgumentException if {@code key} or {@code value} is {@code null}.
   */
  public SessionContextBuilder setOption(String key, String value) {
    if (key == null) {
      throw new IllegalArgumentException("setOption key must be non-null");
    }
    if (value == null) {
      throw new IllegalArgumentException("setOption value must be non-null");
    }
    // remove-then-put so a re-issued key moves to the end of the iteration
    // order. LinkedHashMap.put on an existing key updates the value but keeps
    // the original slot, which would re-order the wire-format relative to
    // intervening keys. For overlapping side-effect keys (e.g.
    // datafusion.optimizer.enable_dynamic_filter_pushdown rewrites the
    // per-operator enable_*_dynamic_filter_pushdown flags) the umbrella key
    // could otherwise apply *after* a later override of one of its
    // sub-flags, silently undoing the override.
    this.options.remove(key);
    this.options.put(key, value);
    return this;
  }

  /**
   * Apply every entry of {@code entries} via {@link #setOption(String, String)}, in {@link
   * LinkedHashMap} insertion order. Use this overload when you need the strict last-write-wins
   * ordering guarantee for overlapping side-effect keys (e.g. {@code
   * datafusion.optimizer.enable_dynamic_filter_pushdown} rewrites the per-operator {@code
   * enable_*_dynamic_filter_pushdown} flags, so a per-operator override must come after the
   * umbrella).
   *
   * @throws IllegalArgumentException if any key or value in {@code entries} is {@code null}.
   */
  public SessionContextBuilder setOptions(LinkedHashMap<String, String> entries) {
    if (entries == null) {
      throw new IllegalArgumentException("setOptions entries must be non-null");
    }
    for (Map.Entry<String, String> e : entries.entrySet()) {
      setOption(e.getKey(), e.getValue());
    }
    return this;
  }

  /**
   * Apply every entry of {@code entries} via {@link #setOption(String, String)}. Iterates in
   * whatever order the supplied {@link Map} produces, which for {@link java.util.HashMap} or {@link
   * java.util.Properties} is unspecified.
   *
   * <p>This is the right overload for the common case where the caller's keys don't overlap with
   * any upstream setter's side effects. If you do need order — see the {@link
   * #setOptions(LinkedHashMap)} overload, which the compiler will resolve to automatically when you
   * pass a {@code LinkedHashMap}.
   *
   * @throws IllegalArgumentException if any key or value in {@code entries} is {@code null}.
   */
  public SessionContextBuilder setOptions(Map<String, String> entries) {
    if (entries == null) {
      throw new IllegalArgumentException("setOptions entries must be non-null");
    }
    for (Map.Entry<String, String> e : entries.entrySet()) {
      setOption(e.getKey(), e.getValue());
    }
    return this;
  }

  /**
   * Construct a {@link SessionContext} with the configured options.
   *
   * @throws RuntimeException if the native side fails to construct the context.
   */
  public SessionContext build() {
    return new SessionContext(toBytes());
  }

  byte[] toBytes() {
    SessionOptions.Builder b = SessionOptions.newBuilder();
    if (batchSize != null) {
      b.setBatchSize(batchSize);
    }
    if (targetPartitions != null) {
      b.setTargetPartitions(targetPartitions);
    }
    if (collectStatistics != null) {
      b.setCollectStatistics(collectStatistics);
    }
    if (informationSchema != null) {
      b.setInformationSchema(informationSchema);
    }
    if (memoryLimitBytes != null && memoryLimitFraction != null) {
      b.setMemoryLimit(
          MemoryLimit.newBuilder()
              .setMaxMemoryBytes(memoryLimitBytes)
              .setMemoryFraction(memoryLimitFraction)
              .build());
    }
    if (tempDirectory != null) {
      b.setTempDirectory(tempDirectory);
    }
    for (Map.Entry<String, String> e : options.entrySet()) {
      b.addOptions(ConfigOption.newBuilder().setKey(e.getKey()).setValue(e.getValue()).build());
    }
    return b.build().toByteArray();
  }
}
