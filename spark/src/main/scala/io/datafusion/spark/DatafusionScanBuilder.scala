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

import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownRequiredColumns, SupportsPushDownV2Filters}
import org.apache.spark.sql.types.StructType

/**
 * ScanBuilder with V2 Predicate pushdown + column pruning. Every translatable predicate is
 * marked Exact and dropped from Spark's post-scan Filter; the rest stay residual.
 *
 * Pushdown discipline: over-claiming Exact = wrong results, under-claiming = full scans. The
 * translator (see [[SparkPredicateTranslator]]) only emits proto for predicates it can encode
 * losslessly — anything else returns `None` and lands in residuals.
 */
class DatafusionScanBuilder(
    factoryFqcn: String,
    optionsProtoBytes: Array[Byte],
    fullSchema: StructType
) extends ScanBuilder
    with SupportsPushDownV2Filters
    with SupportsPushDownRequiredColumns {

  private var pushed: Array[Predicate] = Array.empty
  private var pushedBytes: Array[Array[Byte]] = Array.empty
  private var pruned: StructType = fullSchema

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    val pushedBuf = scala.collection.mutable.ArrayBuffer[Predicate]()
    val bytesBuf = scala.collection.mutable.ArrayBuffer[Array[Byte]]()
    val residual = scala.collection.mutable.ArrayBuffer[Predicate]()

    var i = 0
    while (i < predicates.length) {
      val p = predicates(i)
      SparkPredicateTranslator.translate(p) match {
        case Some(node) =>
          pushedBuf += p
          bytesBuf += node.toByteArray
        case None =>
          residual += p
      }
      i += 1
    }
    pushed = pushedBuf.toArray
    pushedBytes = bytesBuf.toArray
    residual.toArray
  }

  override def pushedPredicates(): Array[Predicate] = pushed

  override def pruneColumns(requiredSchema: StructType): Unit = {
    pruned = requiredSchema
  }

  override def build(): Scan =
    new DatafusionScan(factoryFqcn, optionsProtoBytes, fullSchema, pruned, pushed, pushedBytes)
}
