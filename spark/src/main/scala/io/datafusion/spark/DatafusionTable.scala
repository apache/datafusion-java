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

import java.util

import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Read-only DataFusion-backed table. Capabilities advertise only batch read.
 */
class DatafusionTable(
    val factoryFqcn: String,
    val optionsBytes: Array[Byte],
    val sparkSchema: StructType
) extends Table
    with SupportsRead {

  override def name(): String = s"datafusion.${factoryFqcn.split('.').last}"

  override def schema(): StructType = sparkSchema

  override def capabilities(): util.Set[TableCapability] = {
    val caps = new util.HashSet[TableCapability]()
    caps.add(TableCapability.BATCH_READ)
    caps
  }

  override def newScanBuilder(scanOpts: CaseInsensitiveStringMap): ScanBuilder =
    new DatafusionScanBuilder(factoryFqcn, optionsBytes, sparkSchema)
}
