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

package org.apache.datafusion.spark;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.OptionalInt;

import org.apache.arrow.adbc.driver.jni.JniDriver;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Connector options for the {@code adbc-datafusion} data source, decoded from the Spark options
 * map.
 *
 * <p>Serializable so it can ride along inside an {@link AdbcInputPartition} to each executor, which
 * reopens its own ADBC database/connection. Three keys are connector-control; everything else is
 * forwarded verbatim as native ADBC database options (so provider-specific options pass straight
 * through to the registered {@code TableProvider}).
 *
 * <ul>
 *   <li>{@code driver} (required) -- path to, or name of, the native ADBC driver shared library
 *       (our DataFusion ADBC cdylib). Resolved by the C driver manager.
 *   <li>{@code manifest.path} (optional) -- extra search path for ADBC driver manifests, when the
 *       driver is given by manifest name rather than an absolute path.
 *   <li>{@code table} (required) -- the table name the registered provider exposes; becomes the
 *       Substrait {@code NamedScan} target.
 * </ul>
 */
final class AdbcOptions implements Serializable {

  private static final long serialVersionUID = 1L;

  static final String DRIVER = "driver";
  static final String TABLE = "table";
  static final String TARGET_PARTITIONS = "target_partitions";

  private final String driver;
  private final String table;
  // DataFusion target_partitions, or null to default to the cluster parallelism.
  // Stored as a (Serializable) Integer rather than OptionalInt -- this object rides
  // to executors inside AdbcInputPartition, and OptionalInt is not Serializable.
  private final Integer targetPartitions;
  // Provider-specific / passthrough ADBC database options.
  private final LinkedHashMap<String, String> databaseOptions;

  private AdbcOptions(
      String driver,
      String table,
      Integer targetPartitions,
      LinkedHashMap<String, String> databaseOptions) {
    this.driver = driver;
    this.table = table;
    this.targetPartitions = targetPartitions;
    this.databaseOptions = databaseOptions;
  }

  static AdbcOptions fromOptions(CaseInsensitiveStringMap options) {
    String driver = require(options, DRIVER);
    String table = require(options, TABLE);
    Integer targetPartitions =
        parsePositiveInt(options, TARGET_PARTITIONS).isPresent()
            ? parsePositiveInt(options, TARGET_PARTITIONS).getAsInt()
            : null;

    LinkedHashMap<String, String> passthrough = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String key = entry.getKey();
      if (key.equalsIgnoreCase(DRIVER)
          || key.equalsIgnoreCase(TABLE)
          || key.equalsIgnoreCase(TARGET_PARTITIONS)) {
        continue;
      }
      passthrough.put(key, entry.getValue());
    }
    return new AdbcOptions(driver, table, targetPartitions, passthrough);
  }

  String table() {
    return table;
  }

  /** Explicit DataFusion target_partitions, or empty to use the cluster parallelism. */
  OptionalInt targetPartitions() {
    return targetPartitions == null ? OptionalInt.empty() : OptionalInt.of(targetPartitions);
  }

  /**
   * Build the parameter map for {@link JniDriver#open}. All values must be strings; the {@code
   * jni.driver} key (set via {@link JniDriver#PARAM_DRIVER}) is consumed by the driver manager to
   * locate the shared library, the rest become native database options.
   */
  Map<String, Object> driverParameters() {
    Map<String, Object> params = new LinkedHashMap<>();
    JniDriver.PARAM_DRIVER.set(params, driver);
    params.putAll(databaseOptions);
    return params;
  }

  /**
   * Identity of the native ADBC database these options open. Two option sets that produce the same
   * {@link #driverParameters()} share a key, so {@link AdbcConnectionPool} caches one native
   * database/connection per executor JVM for them. Derived from exactly the inputs {@code
   * driverParameters()} consumes -- the {@code driver} path/name plus the passthrough database
   * options -- and must stay adjacent to it so the two cannot drift. {@code table} and {@code
   * target_partitions} are connector-control (not forwarded to the native database) and so are
   * deliberately excluded.
   */
  AdbcConnectionPool.Key cacheKey() {
    return new AdbcConnectionPool.Key(driver, databaseOptions);
  }

  private static String require(CaseInsensitiveStringMap options, String key) {
    String value = options.getOrDefault(key, null);
    if (value == null || value.isEmpty()) {
      throw new IllegalArgumentException(
          "the 'adbc-datafusion' data source requires the '" + key + "' option");
    }
    return value;
  }

  private static OptionalInt parsePositiveInt(CaseInsensitiveStringMap options, String key) {
    String value = options.getOrDefault(key, null);
    if (value == null || value.isEmpty()) {
      return OptionalInt.empty();
    }
    int parsed;
    try {
      parsed = Integer.parseInt(value.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("'" + key + "' must be an integer, got: " + value, e);
    }
    if (parsed < 1) {
      throw new IllegalArgumentException("'" + key + "' must be >= 1, got: " + parsed);
    }
    return OptionalInt.of(parsed);
  }
}
