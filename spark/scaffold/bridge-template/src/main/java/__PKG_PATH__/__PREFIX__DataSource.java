package __PKG__;

import io.datafusion.spark.DatafusionSource;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Gives the bridge its Spark format name: {@code spark.read.format("__FORMAT__")}. Registered via
 * {@code META-INF/services/org.apache.spark.sql.sources.DataSourceRegister}.
 */
public class __PREFIX__DataSource extends DatafusionSource {

  @Override
  public String shortName() {
    return "__FORMAT__";
  }

  @Override
  public String factoryFqcn(CaseInsensitiveStringMap options) {
    return __PREFIX__ProviderFactory.class.getName();
  }
}
