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

import java.util.Map;

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.driver.jni.JniDriver;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Entry point for the {@code adbc-datafusion} Spark data source.
 *
 * <p>The native boundary is a standard ADBC driver: the arrow-adbc Java driver manager loading a
 * DataFusion ADBC cdylib. {@code spark.read.format("adbc-datafusion").option("driver",
 * ...).option("table", ...).load()} resolves here; the schema is probed once, on the driver, via
 * {@link AdbcConnection#getTableSchema}.
 */
public final class AdbcDatafusionTableProvider implements TableProvider, DataSourceRegister {

  @Override
  public String shortName() {
    return "adbc-datafusion";
  }

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    AdbcOptions opts = AdbcOptions.fromOptions(options);
    try (BufferAllocator allocator = new RootAllocator();
        AdbcDatabase db = new JniDriver(allocator).open(opts.driverParameters());
        AdbcConnection conn = db.connect()) {
      Schema arrow = conn.getTableSchema(null, null, opts.table());
      return SchemaConverter.toSparkSchema(arrow);
    } catch (Exception e) {
      throw new RuntimeException("failed to probe ADBC schema for table " + opts.table(), e);
    }
  }

  @Override
  public Table getTable(
      StructType schema, Transform[] partitioning, Map<String, String> properties) {
    AdbcOptions opts = AdbcOptions.fromOptions(new CaseInsensitiveStringMap(properties));
    return new AdbcTable(schema, opts);
  }

  @Override
  public boolean supportsExternalMetadata() {
    return false;
  }
}
