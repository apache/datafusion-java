package __PKG__;

import io.datafusion.spark.NativeLibraryLoader;

/**
 * JNI surface generated on the Rust side by {@code export_bridge!} with {@code jni_class =
 * "__JNI_CLASS__"} — the mangled binary name of THIS class. Renaming or moving this class
 * requires regenerating the Rust macro invocation to match.
 *
 * <p>The cdylib is bundled in this jar under {@code __PKG_PATH__/<os>/<arch>/} (see the antrun
 * execution in pom.xml) and extracted/loaded once per JVM by the connector's loader.
 */
final class BridgeNative {

  private BridgeNative() {}

  static {
    NativeLibraryLoader.load(BridgeNative.class, "__PKG_PATH__", "__LIB__");
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
