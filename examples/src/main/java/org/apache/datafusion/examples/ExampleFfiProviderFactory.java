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
import io.datafusion.spark.PartitionInfo;

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
 *   <li>{@code num_batches} — number of in-memory {@code RecordBatch}es composing the table.
 *       Default {@code 1}.
 *   <li>{@code num_partitions} — number of DataFusion-native MemTable partitions the batches are
 *       distributed across (round-robin). Default {@code 1}. Mostly interesting together with
 *       {@code shared_scan}.
 *   <li>{@code shared_scan} — {@code true} opts into the connector's shared-scan mode: one cached
 *       provider + plan per executor, one Spark task per MemTable partition. Default {@code false}
 *       (single task via {@link #listPartitions(byte[])}).
 * </ul>
 *
 * <p>Real bridges (Rerun, HDF5, custom Iceberg) use a protobuf schema for {@code
 * optionsProtoBytes}; this example uses a hand-rolled length-prefixed binary format to keep the
 * wire layer obvious:
 *
 * <pre>
 *   [u32 LE name_prefix_len][name_prefix UTF-8 bytes][u32 LE num_rows][u32 LE num_batches]
 *       [u32 LE num_partitions][u8 shared_scan]
 * </pre>
 *
 * <p>An empty {@code byte[]} is also accepted by the native side and decoded as all defaults; the
 * two trailing fields are optional so older blobs keep decoding.
 *
 * <p>In the default mode a single partition (id {@code "p0"}, empty {@code partitionBytes}, no
 * preferred host) is reported so Spark spawns one task; the executor calls {@link
 * #createProvider(byte[], byte[])} to obtain a fresh {@code FFI_TableProvider} pointer, hands it to
 * {@link org.apache.datafusion.SessionContext#registerFfiTable(String, long)}, and streams the
 * resulting Arrow record batches back into the Spark scan.
 */
public final class ExampleFfiProviderFactory implements FfiProviderFactory {

  static final String OPT_NAME_PREFIX = "name_prefix";
  static final String OPT_NUM_ROWS = "num_rows";
  static final String OPT_NUM_BATCHES = "num_batches";
  static final String OPT_NUM_PARTITIONS = "num_partitions";
  static final String OPT_SHARED_SCAN = "shared_scan";

  static final String DEFAULT_NAME_PREFIX = "row";
  static final int DEFAULT_NUM_ROWS = 4;
  static final int DEFAULT_NUM_BATCHES = 1;
  static final int DEFAULT_NUM_PARTITIONS = 1;

  public ExampleFfiProviderFactory() {}

  @Override
  public byte[] encodeOptions(Map<String, String> sparkOptions) {
    String namePrefix = sparkOptions.getOrDefault(OPT_NAME_PREFIX, DEFAULT_NAME_PREFIX);
    int numRows = parsePositiveInt(sparkOptions, OPT_NUM_ROWS, DEFAULT_NUM_ROWS);
    int numBatches = parsePositiveInt(sparkOptions, OPT_NUM_BATCHES, DEFAULT_NUM_BATCHES);
    int numPartitions = parsePositiveInt(sparkOptions, OPT_NUM_PARTITIONS, DEFAULT_NUM_PARTITIONS);
    boolean sharedScan = Boolean.parseBoolean(sparkOptions.getOrDefault(OPT_SHARED_SCAN, "false"));

    byte[] nameBytes = namePrefix.getBytes(StandardCharsets.UTF_8);
    ByteBuffer buf =
        ByteBuffer.allocate(4 + nameBytes.length + 4 + 4 + 4 + 1).order(ByteOrder.LITTLE_ENDIAN);
    buf.putInt(nameBytes.length);
    buf.put(nameBytes);
    buf.putInt(numRows);
    buf.putInt(numBatches);
    buf.putInt(numPartitions);
    buf.put((byte) (sharedScan ? 1 : 0));
    return buf.array();
  }

  @Override
  public PartitionInfo[] listPartitions(byte[] optionsProtoBytes) {
    // Single partition; the example MemTable is not actually sliced. A real bridge would
    // populate `partitionBytes` per slice and `preferredLocations` with the hosts holding it.
    return new PartitionInfo[] {new PartitionInfo("p0", new byte[0], new String[0])};
  }

  @Override
  public PartitionInfo[] listPartitions(byte[] optionsProtoBytes, byte[][] filterProtoBytes) {
    // The example cannot prune its single partition, but a real bridge would inspect the
    // pushed predicates here and drop partitions that cannot match.
    System.out.println(
        "ExampleFfiProviderFactory.listPartitions received "
            + filterProtoBytes.length
            + " pushed filter(s)");
    return listPartitions(optionsProtoBytes);
  }

  @Override
  public boolean sharedScan(byte[] optionsProtoBytes) {
    // The flag is the final byte of the options blob (present only when the encoder wrote the
    // trailing fields). The bridge owns its wire format, so decoding it here is fair game.
    return optionsProtoBytes != null
        && optionsProtoBytes.length >= 1
        && hasTrailingFields(optionsProtoBytes)
        && optionsProtoBytes[optionsProtoBytes.length - 1] == 1;
  }

  private static boolean hasTrailingFields(byte[] bytes) {
    if (bytes.length < 4) {
      return false;
    }
    int nameLen = ByteBuffer.wrap(bytes, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
    // base layout: 4 (len) + name + 4 (num_rows) + 4 (num_batches); trailing adds 4 + 1.
    return bytes.length >= 4 + nameLen + 8 + 5;
  }

  @Override
  public long createProvider(byte[] optionsProtoBytes, byte[] partitionBytes) {
    // The example bridge has no per-partition state; `partitionBytes` is ignored.
    // The print makes provider-build amortization observable in the demo: shared-scan
    // mode builds once per (executor x query) regardless of task count, while the
    // per-partition path builds once per task.
    System.out.println("ExampleFfiProviderFactory.createProvider building a MemTable provider");
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
