package __PKG__;

import io.datafusion.spark.ScanBackend;

/** Routes the connector's scan calls to this bridge's own cdylib. Pure delegation. */
public final class __PREFIX__ScanBackend implements ScanBackend {

  @Override
  public byte[] providerSchemaIpc(byte[] options, byte[] partitionBytes) {
    return BridgeNative.providerSchemaIpc(options, partitionBytes);
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
    return BridgeNative.createScan(
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
    return BridgeNative.partitionCount(scanHandle);
  }

  @Override
  public void executeStreamPartition(long scanHandle, int partition, long ffiStreamAddr) {
    BridgeNative.executeStreamPartition(scanHandle, partition, ffiStreamAddr);
  }

  @Override
  public void executeStream(long scanHandle, long ffiStreamAddr) {
    BridgeNative.executeStream(scanHandle, ffiStreamAddr);
  }

  @Override
  public void closeScan(long scanHandle) {
    BridgeNative.closeScan(scanHandle);
  }
}
