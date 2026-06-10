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

import org.apache.spark.sql.connector.read.InputPartition

/**
 * Per-task payload shipped from driver to executor via Java serialization.
 *
 *  - `factoryFqcn`: fully-qualified class name of the bridge's `FfiProviderFactory`. The
 *    executor reflectively instantiates this and calls `createProvider(optionsProtoBytes)`.
 *  - `optionsProtoBytes`: bridge-specific connection options, encoded by the bridge. Opaque to
 *    connector-core.
 *  - `projectionColumnNames`: pruned column list (post-`pruneColumns`).
 *  - `filterProtoBytes`: V2 `Predicate` → DataFusion `LogicalExprNode` proto bytes; each one is
 *    applied via `DataFrame.filterFromProto`.
 *  - `partitionId`: stable identifier (e.g. Rerun segment id) — for `preferredLocations` / debug.
 */
final case class DatafusionInputPartition(
    factoryFqcn: String,
    optionsProtoBytes: Array[Byte],
    projectionColumnNames: Array[String],
    filterProtoBytes: Array[Array[Byte]],
    partitionId: String
) extends InputPartition
