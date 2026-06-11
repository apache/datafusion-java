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

import org.scalatest.funsuite.AnyFunSuite

class FfiProviderFactoryDefaultsTest extends AnyFunSuite {

  /** Factory overriding only listPartitions (to spy on its inputs). */
  private class MinimalFactory extends FfiProviderFactory {
    var lastListPartitionsOpts: Array[Byte] = _

    override def listPartitions(optionsProtoBytes: Array[Byte]): Array[PartitionInfo] = {
      lastListPartitionsOpts = optionsProtoBytes
      Array(new PartitionInfo("p0", Array.emptyByteArray, Array.empty[String]))
    }
  }

  /** Every method left at its default — the literal minimum a bridge can ship. */
  private class EmptyFactory extends FfiProviderFactory

  test("sharedScan defaults to false") {
    assert(!new MinimalFactory().sharedScan(Array[Byte](1, 2, 3)))
  }

  test("default encodeOptions uses OptionsCodec") {
    val opts = new java.util.HashMap[String, String]()
    opts.put("url", "grpc://h:1")
    val bytes = new EmptyFactory().encodeOptions(opts)
    assert(java.util.Arrays.equals(bytes, OptionsCodec.encode(opts)))
    assert(OptionsCodec.decode(bytes).get("url") == "grpc://h:1")
  }

  test("default listPartitions reports a single whole-dataset partition") {
    val partitions = new EmptyFactory().listPartitions(Array[Byte](1))
    assert(partitions.length == 1)
    assert(partitions(0).id == "p0")
    assert(partitions(0).partitionBytes().isEmpty)
    assert(partitions(0).preferredLocations().isEmpty)
  }

  test("default createProvider rejects with guidance toward scanBackend") {
    val e = intercept[UnsupportedOperationException] {
      new EmptyFactory().createProvider(Array.emptyByteArray, Array.emptyByteArray)
    }
    assert(e.getMessage.contains("scanBackend"))
  }

  test("default scanBackend is the FFI path") {
    assert(new EmptyFactory().scanBackend().isInstanceOf[FfiScanBackend])
  }

  test("filter-aware listPartitions delegates to the filter-unaware overload") {
    val factory = new MinimalFactory
    val opts = Array[Byte](7, 8)
    val filters = Array(Array[Byte](1), Array[Byte](2))
    val partitions = factory.listPartitions(opts, filters)
    assert(partitions.length == 1)
    assert(partitions(0).id == "p0")
    assert(factory.lastListPartitionsOpts eq opts)
  }

  test("reportPartitioning defaults to null") {
    assert(new MinimalFactory().reportPartitioning(Array.emptyByteArray) == null)
  }

  test("PartitionInfo 3-arg constructor leaves partitionKeyValues null") {
    val p = new PartitionInfo("p0", Array.emptyByteArray, Array.empty[String])
    assert(p.partitionKeyValues() == null)
  }

  test("PartitionInfo 4-arg constructor carries key values") {
    val p = new PartitionInfo(
      "p0",
      Array.emptyByteArray,
      Array.empty[String],
      Array[AnyRef]("segment-a", Long.box(42L)))
    assert(p.partitionKeyValues().length == 2)
    assert(p.partitionKeyValues()(0) == "segment-a")
  }
}
