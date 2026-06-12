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

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

import org.scalatest.funsuite.AnyFunSuite

class OptionsCodecTest extends AnyFunSuite {

  /**
   * Shared fixture: must stay byte-identical to the one asserted by the Rust-side
   * `datafusion_spark_bridge::options` tests. {"table": "t1", "url": "grpc://h:1"} encodes
   * (sorted: table < url) as below.
   */
  private def fixtureBytes(): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    def writeInt(v: Int): Unit = {
      out.write((v >>> 24) & 0xFF); out.write((v >>> 16) & 0xFF)
      out.write((v >>> 8) & 0xFF); out.write(v & 0xFF)
    }
    def writeString(s: String): Unit = {
      val b = s.getBytes(StandardCharsets.UTF_8)
      writeInt(b.length)
      out.write(b, 0, b.length)
    }
    writeInt(2)
    Seq("table" -> "t1", "url" -> "grpc://h:1").foreach { case (k, v) =>
      writeString(k); writeString(v)
    }
    out.toByteArray
  }

  test("encodes the cross-language fixture byte-identically, sorted by key") {
    // Insertion order deliberately unsorted; encoding must sort.
    val opts = new java.util.LinkedHashMap[String, String]()
    opts.put("url", "grpc://h:1")
    opts.put("table", "t1")
    assert(java.util.Arrays.equals(OptionsCodec.encode(opts), fixtureBytes()))
  }

  test("round-trips including unicode values") {
    val opts = new java.util.HashMap[String, String]()
    opts.put("a", "1")
    opts.put("unicode", "héllo→world")
    val decoded = OptionsCodec.decode(OptionsCodec.encode(opts))
    assert(decoded.size() == 2)
    assert(decoded.get("unicode") == "héllo→world")
  }

  test("null and empty maps encode to a zero count and decode back empty") {
    assert(OptionsCodec.decode(OptionsCodec.encode(null)).isEmpty)
    assert(OptionsCodec.decode(Array.emptyByteArray).isEmpty)
  }

  test("rejects truncation and trailing bytes") {
    val bytes = fixtureBytes()
    intercept[IllegalArgumentException] {
      OptionsCodec.decode(java.util.Arrays.copyOf(bytes, bytes.length - 1))
    }
    intercept[IllegalArgumentException] {
      OptionsCodec.decode(java.util.Arrays.copyOf(bytes, bytes.length + 1))
    }
  }

  test("rejects null keys or values") {
    val opts = new java.util.HashMap[String, String]()
    opts.put("k", null)
    intercept[IllegalArgumentException] { OptionsCodec.encode(opts) }
  }
}
