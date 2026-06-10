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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import io.datafusion.spark.FfiProviderFactory;

/**
 * Minimal {@link FfiProviderFactory} that exposes the example {@code MemTable} produced by {@link
 * FfiTableProviderExampleNative#createMemTableProvider(byte[])} as a Spark DataSource V2 source.
 *
 * <p>Wire it into PySpark with:
 *
 * <pre>{@code
 * df = (spark.read.format("datafusion")
 *         .option("df.factory", "org.apache.datafusion.examples.ExampleFfiProviderFactory")
 *         .option("name_prefix", "user")
 *         .option("num_rows", "5")
 *         .option("num_batches", "3")
 *         .load())
 * }</pre>
 *
 * <p>Supported options (all optional):
 *
 * <ul>
 *   <li>{@code name_prefix} — prefix string used for generated {@code name} column values. Default
 *       {@code "row"}.
 *   <li>{@code num_rows} — rows per batch. Default {@code 4}.
 *   <li>{@code num_batches} — number of in-memory {@code RecordBatch}es composing the table. Default
 *       {@code 1}.
 * </ul>
 *
 * <p>Real bridges (Rerun, HDF5, custom Iceberg) use a protobuf schema for {@code optionsProtoBytes};
 * this example uses a hand-rolled length-prefixed binary format to keep the wire layer obvious:
 *
 * <pre>
 *   [u32 LE name_prefix_len][name_prefix UTF-8 bytes][u32 LE num_rows][u32 LE num_batches]
 * </pre>
 *
 * <p>An empty {@code byte[]} is also accepted by the native side and decoded as all defaults.
 *
 * <p>A single partition (id {@code "p0"}) is reported so Spark spawns one task; the executor calls
 * {@link #createProvider(byte[])} to obtain a fresh {@code FFI_TableProvider} pointer, hands it to
 * {@link org.apache.datafusion.SessionContext#registerFfiTable(String, long)}, and streams the
 * resulting Arrow record batches back into the Spark scan.
 */
public final class ExampleFfiProviderFactory implements FfiProviderFactory {

  static final String OPT_NAME_PREFIX = "name_prefix";
  static final String OPT_NUM_ROWS = "num_rows";
  static final String OPT_NUM_BATCHES = "num_batches";

  static final String DEFAULT_NAME_PREFIX = "row";
  static final int DEFAULT_NUM_ROWS = 4;
  static final int DEFAULT_NUM_BATCHES = 1;

  public ExampleFfiProviderFactory() {}

  @Override
  public byte[] encodeOptions(Map<String, String> sparkOptions) {
    String namePrefix = sparkOptions.getOrDefault(OPT_NAME_PREFIX, DEFAULT_NAME_PREFIX);
    int numRows = parsePositiveInt(sparkOptions, OPT_NUM_ROWS, DEFAULT_NUM_ROWS);
    int numBatches = parsePositiveInt(sparkOptions, OPT_NUM_BATCHES, DEFAULT_NUM_BATCHES);

    byte[] nameBytes = namePrefix.getBytes(StandardCharsets.UTF_8);
    ByteBuffer buf = ByteBuffer.allocate(4 + nameBytes.length + 4 + 4).order(ByteOrder.LITTLE_ENDIAN);
    buf.putInt(nameBytes.length);
    buf.put(nameBytes);
    buf.putInt(numRows);
    buf.putInt(numBatches);
    return buf.array();
  }

  @Override
  public String[] listPartitions(byte[] optionsProtoBytes) {
    return new String[] {"p0"};
  }

  @Override
  public long createProvider(byte[] optionsProtoBytes) {
    return FfiTableProviderExampleNative.createMemTableProvider(optionsProtoBytes);
  }

  private static int parsePositiveInt(Map<String, String> opts, String key, int defaultValue) {
    String raw = opts.get(key);
    if (raw == null || raw.isEmpty()) {
      return defaultValue;
    }
    int parsed;
    try {
      parsed = Integer.parseInt(raw.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "ExampleFfiProviderFactory: option '" + key + "' must be an integer, got: " + raw);
    }
    if (parsed <= 0) {
      throw new IllegalArgumentException(
          "ExampleFfiProviderFactory: option '" + key + "' must be > 0, got: " + parsed);
    }
    return parsed;
  }
}
