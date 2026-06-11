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

/**
 * Generic FFI {@link ScanBackend}: asks the factory for a raw {@code FFI_TableProvider} pointer and
 * routes everything through the connector's own cdylib ({@link FfiHelperNative}). This is the
 * {@link FfiProviderFactory#scanBackend()} default; bridges that statically link their provider via
 * {@code export_bridge!} replace it with a backend delegating to their own native class.
 */
public final class FfiScanBackend implements ScanBackend {

  private final FfiProviderFactory factory;

  public FfiScanBackend(FfiProviderFactory factory) {
    this.factory = factory;
  }

  @Override
  public byte[] providerSchemaIpc(byte[] options, byte[] partitionBytes) {
    long ptr = factory.createProvider(options, partitionBytes);
    return FfiHelperNative.providerSchemaIpc(ptr);
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
    long ptr = factory.createProvider(options, partitionBytes);
    return FfiHelperNative.createScan(
        ptr,
        targetPartitions,
        batchSize,
        optionKeys,
        optionValues,
        projectionColumns,
        filterProtos);
  }

  @Override
  public int partitionCount(long scanHandle) {
    return FfiHelperNative.partitionCount(scanHandle);
  }

  @Override
  public void executeStreamPartition(long scanHandle, int partition, long ffiStreamAddr) {
    FfiHelperNative.executeStreamPartition(scanHandle, partition, ffiStreamAddr);
  }

  @Override
  public void executeStream(long scanHandle, long ffiStreamAddr) {
    FfiHelperNative.executeStream(scanHandle, ffiStreamAddr);
  }

  @Override
  public void closeScan(long scanHandle) {
    FfiHelperNative.closeScan(scanHandle);
  }
}
