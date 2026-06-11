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

import io.datafusion.spark.ScanBackend;

/** Routes the connector's scan calls to the example bridge cdylib. Pure delegation. */
final class ExampleScanBackend implements ScanBackend {

  @Override
  public byte[] providerSchemaIpc(byte[] options, byte[] partitionBytes) {
    return ExampleBridgeNative.providerSchemaIpc(options, partitionBytes);
  }

  @Override
  public long createScan(
      byte[] options,
      byte[] partitionBytes,
      int targetPartitions,
      int batchSize,
      String[] optionKeys,
      String[] optionValues,
      String[] projectionColumns,
      byte[][] filterProtos) {
    return ExampleBridgeNative.createScan(
        options,
        partitionBytes,
        targetPartitions,
        batchSize,
        optionKeys,
        optionValues,
        projectionColumns,
        filterProtos);
  }

  @Override
  public int partitionCount(long scanHandle) {
    return ExampleBridgeNative.partitionCount(scanHandle);
  }

  @Override
  public void executeStreamPartition(long scanHandle, int partition, long ffiStreamAddr) {
    ExampleBridgeNative.executeStreamPartition(scanHandle, partition, ffiStreamAddr);
  }

  @Override
  public void executeStream(long scanHandle, long ffiStreamAddr) {
    ExampleBridgeNative.executeStream(scanHandle, ffiStreamAddr);
  }

  @Override
  public void closeScan(long scanHandle) {
    ExampleBridgeNative.closeScan(scanHandle);
  }
}
