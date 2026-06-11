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

/** Shared SQL construction for the per-task (legacy) and shared-scan read paths. */
private[spark] object DatafusionSqlBuilder {

  /** Registration name for the per-task provider in legacy mode. */
  val PartitionTableName = "df_spark_partition"

  /** Registration name for the per-executor provider in shared-scan mode. */
  val SharedTableName = "df_spark_shared"

  /** `SELECT <quoted projection or *> FROM "<table>"`. */
  def buildSql(projectionColumnNames: Array[String], tableName: String): String = {
    val cols =
      if (projectionColumnNames.isEmpty) "*"
      else
        projectionColumnNames
          .map(c => "\"" + c.replace("\"", "\"\"") + "\"")
          .mkString(", ")
    s"""SELECT $cols FROM "$tableName""""
  }
}
