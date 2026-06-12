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

import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite

class PartitionKeyConversionTest extends AnyFunSuite {

  private def info(id: String, keys: Array[AnyRef]): PartitionInfo =
    new PartitionInfo(id, Array.emptyByteArray, Array.empty[String], keys)

  private def infoNoKeys(id: String): PartitionInfo =
    new PartitionInfo(id, Array.emptyByteArray, Array.empty[String])

  test("String and Long key values convert to catalyst representations") {
    val reported = ReportedPartitioning.identity("segment_id", "bucket")
    val row =
      DatafusionBatch.toKeyRow("p0", Array[AnyRef]("segment-a", Long.box(42L)), reported)
    assert(row.numFields == 2)
    assert(row.get(0, org.apache.spark.sql.types.StringType) == UTF8String.fromString("segment-a"))
    assert(row.getLong(1) == 42L)
  }

  test("arity mismatch between key values and declared keys throws") {
    val reported = ReportedPartitioning.identity("segment_id", "bucket")
    val e = intercept[IllegalStateException] {
      DatafusionBatch.toKeyRow("p0", Array[AnyRef]("only-one"), reported)
    }
    assert(e.getMessage.contains("declares 2 key(s)"))
  }

  test("keyed state requires reported partitioning") {
    val partitions = Array(info("p0", Array[AnyRef]("a")))
    assert(!DatafusionBatch.validateKeyedState("F", partitions, null))
  }

  test("no partitions with keys means unkeyed, even with reported partitioning") {
    val reported = ReportedPartitioning.identity("segment_id")
    val partitions = Array(infoNoKeys("p0"), infoNoKeys("p1"))
    assert(!DatafusionBatch.validateKeyedState("F", partitions, reported))
  }

  test("all partitions with keys means keyed") {
    val reported = ReportedPartitioning.identity("segment_id")
    val partitions =
      Array(info("p0", Array[AnyRef]("a")), info("p1", Array[AnyRef]("b")))
    assert(DatafusionBatch.validateKeyedState("F", partitions, reported))
  }

  test("mixed keyed and unkeyed partitions throw driver-side") {
    val reported = ReportedPartitioning.identity("segment_id")
    val partitions = Array(info("p0", Array[AnyRef]("a")), infoNoKeys("p1"))
    val e = intercept[IllegalStateException] {
      DatafusionBatch.validateKeyedState("F", partitions, reported)
    }
    assert(e.getMessage.contains("only 1 of 2"))
  }
}
