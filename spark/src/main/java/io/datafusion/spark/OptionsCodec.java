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

package io.datafusion.spark;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Default wire format for {@link BridgeProviderFactory#encodeOptions(Map)}: the Spark options map
 * as length-prefixed UTF-8 pairs, <b>sorted by key</b>.
 *
 * <p>Layout (all integers big-endian {@code int32}): entry count, then per entry key length, key
 * bytes, value length, value bytes. Key-sorting makes the bytes a pure function of the map's
 * contents regardless of source iteration order — required by the shared-scan determinism contract,
 * where the options bytes are the cache/plan identity.
 *
 * <p>The Rust decoder lives in {@code datafusion_spark_bridge::options}; bridges using the default
 * {@code encodeOptions} read their options there as a {@code BTreeMap<String, String>}. The two
 * implementations are pinned to each other by a shared test fixture.
 */
public final class OptionsCodec {

  private OptionsCodec() {}

  /** Encode {@code options} sorted by key. {@code null} or empty map encodes as count 0. */
  public static byte[] encode(Map<String, String> options) {
    TreeMap<String, String> sorted = options == null ? new TreeMap<>() : new TreeMap<>(options);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writeInt(out, sorted.size());
    for (Map.Entry<String, String> e : sorted.entrySet()) {
      if (e.getKey() == null || e.getValue() == null) {
        throw new IllegalArgumentException("OptionsCodec does not accept null keys or values");
      }
      writeBytes(out, e.getKey().getBytes(StandardCharsets.UTF_8));
      writeBytes(out, e.getValue().getBytes(StandardCharsets.UTF_8));
    }
    return out.toByteArray();
  }

  /** Decode bytes produced by {@link #encode(Map)}. Preserves the encoded (sorted) order. */
  public static Map<String, String> decode(byte[] bytes) {
    Map<String, String> out = new LinkedHashMap<>();
    if (bytes == null || bytes.length == 0) {
      return out;
    }
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    int count = readCount(buf, "entry count");
    for (int i = 0; i < count; i++) {
      String key = readString(buf, "key of entry " + i);
      String value = readString(buf, "value of entry " + i);
      out.put(key, value);
    }
    if (buf.hasRemaining()) {
      throw new IllegalArgumentException(
          "OptionsCodec: " + buf.remaining() + " trailing byte(s) after " + count + " entries");
    }
    return out;
  }

  private static void writeInt(ByteArrayOutputStream out, int v) {
    out.write((v >>> 24) & 0xFF);
    out.write((v >>> 16) & 0xFF);
    out.write((v >>> 8) & 0xFF);
    out.write(v & 0xFF);
  }

  private static void writeBytes(ByteArrayOutputStream out, byte[] bytes) {
    writeInt(out, bytes.length);
    out.write(bytes, 0, bytes.length);
  }

  private static int readCount(ByteBuffer buf, String what) {
    if (buf.remaining() < 4) {
      throw new IllegalArgumentException("OptionsCodec: truncated " + what);
    }
    int v = buf.getInt();
    if (v < 0) {
      throw new IllegalArgumentException("OptionsCodec: negative " + what + ": " + v);
    }
    return v;
  }

  private static String readString(ByteBuffer buf, String what) {
    int len = readCount(buf, "length of " + what);
    if (buf.remaining() < len) {
      throw new IllegalArgumentException("OptionsCodec: truncated " + what);
    }
    byte[] bytes = new byte[len];
    buf.get(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }
}
