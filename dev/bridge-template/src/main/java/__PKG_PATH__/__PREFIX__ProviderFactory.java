package __PKG__;

import io.datafusion.spark.FfiProviderFactory;
import io.datafusion.spark.ScanBackend;

/**
 * The bridge's contract with the Spark connector. This is a STATIC bridge — the provider is built
 * inside this bridge's own cdylib — so the only required override is {@link #scanBackend()}.
 *
 * <p>Useful optional overrides (see their javadoc on {@link FfiProviderFactory}):
 *
 * <ul>
 *   <li>{@code encodeOptions} — only if you have your own options schema; the default ships the
 *       Spark options map in the connector's {@code OptionsCodec} format, which the Rust side
 *       already decodes via {@code datafusion_spark_bridge::options::decode_options}.
 *   <li>{@code listPartitions} — the default is ONE whole-dataset partition. Override to split
 *       into more Spark tasks (with optional preferred hosts and partition keys), or…
 *   <li>{@code sharedScan} — …opt into shared-scan mode: one provider per executor, one Spark
 *       task per DataFusion output partition. Mind the determinism contract.
 * </ul>
 */
public final class __PREFIX__ProviderFactory implements FfiProviderFactory {

  @Override
  public ScanBackend scanBackend() {
    return new __PREFIX__ScanBackend();
  }
}
