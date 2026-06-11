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

package io.datafusion.spark

import java.io.ByteArrayInputStream
import java.nio.channels.Channels
import java.util

import org.apache.arrow.vector.ipc.ReadChannel
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Generic Spark DataSource V2 entry point. Concrete bridges either:
 *   - Subclass and override [[shortName]] + [[factoryFqcn]] (the short-name shim pattern), or
 *   - Use this class directly with `option("df.factory", "fully.qualified.FactoryClass")`.
 *
 * Schema discovery happens driver-side inside the connector cdylib: the factory's
 * `FFI_TableProvider` is built and handed to `FfiHelperNative.providerSchemaIpc`, which widens it
 * and returns its Arrow schema as IPC bytes. The same `optionsProtoBytes` (and the factory FQCN)
 * is then carried verbatim through `DatafusionInputPartition`, so each executor task repeats the
 * same factory → createScan pipeline locally.
 */
class DatafusionSource extends TableProvider with DataSourceRegister {

  override def shortName(): String = "datafusion"

  /** Spark option key carrying the FfiProviderFactory FQCN when no override is provided. */
  protected val FactoryOptionKey: String = "df.factory"

  /**
   * Resolve the bridge factory class name from the Spark options. Subclasses override to return a
   * hard-coded FQCN so users don't need to set `df.factory` themselves.
   */
  protected def factoryFqcn(options: CaseInsensitiveStringMap): String = {
    val v = options.get(FactoryOptionKey)
    if (v == null || v.isEmpty)
      throw new IllegalArgumentException(
        s"DatafusionSource: option '$FactoryOptionKey' is required when no subclass override is set"
      )
    v
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val fqcn = factoryFqcn(options)
    val factory = instantiateFactory(fqcn)
    val optionsBytes = factory.encodeOptions(options.asCaseSensitiveMap())
    // Schema probe: pass empty partitionBytes — bridges are required to honour an empty
    // payload for the driver-side probe (schema must not depend on per-partition state).
    val rawPtr = factory.createProvider(optionsBytes, Array.emptyByteArray)
    val ipcBytes = FfiHelperNative.providerSchemaIpc(rawPtr)
    val arrowSchema = MessageSerializer.deserializeSchema(
      new ReadChannel(Channels.newChannel(new ByteArrayInputStream(ipcBytes))))
    ArrowToSparkSchema.toSparkSchema(arrowSchema)
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]
  ): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    val fqcn = factoryFqcn(options)
    val factory = instantiateFactory(fqcn)
    val optionsBytes = factory.encodeOptions(options.asCaseSensitiveMap())
    new DatafusionTable(fqcn, optionsBytes, schema)
  }

  override def supportsExternalMetadata(): Boolean = false

  private def instantiateFactory(fqcn: String): FfiProviderFactory = {
    val cls = Class.forName(fqcn)
    cls.getDeclaredConstructor().newInstance().asInstanceOf[FfiProviderFactory]
  }
}
