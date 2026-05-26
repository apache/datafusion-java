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

package org.apache.datafusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.Test;

class SessionContextCacheManagerTest {

  @Test
  void fileMetadataCacheBuildsAndContextIsUsable() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx =
            SessionContext.builder()
                .cacheManager(CacheManagerOptions.builder().fileMetadataCache(64L << 20).build())
                .build();
        DataFrame df = ctx.sql("SELECT 1");
        ArrowReader reader = df.collect(allocator)) {
      assertTrue(reader.loadNextBatch());
      assertEquals(1, reader.getVectorSchemaRoot().getRowCount());
    }
  }

  @Test
  void listFilesCacheWithFiniteTtlBuilds() {
    try (SessionContext ctx =
        SessionContext.builder()
            .cacheManager(
                CacheManagerOptions.builder()
                    .listFilesCache(8L << 20, Duration.ofMinutes(5))
                    .build())
            .build()) {
      assertNotNull(ctx);
    }
  }

  @Test
  void listFilesCacheWithInfiniteTtlBuilds() {
    // null TTL must reach the Rust side as None (infinite). Construction
    // succeeds; the only failure mode would be the JNI layer mistranslating
    // null as "TTL=0" and the upstream constructor rejecting it.
    try (SessionContext ctx =
        SessionContext.builder()
            .cacheManager(CacheManagerOptions.builder().listFilesCache(8L << 20, null).build())
            .build()) {
      assertNotNull(ctx);
    }
  }

  @Test
  void fileStatisticsCacheBuilds() {
    try (SessionContext ctx =
        SessionContext.builder()
            .cacheManager(CacheManagerOptions.builder().fileStatisticsCache(true).build())
            .build()) {
      assertNotNull(ctx);
    }
  }

  @Test
  void allThreeCachesTogetherBuildAndContextIsUsable() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx =
            SessionContext.builder()
                .cacheManager(
                    CacheManagerOptions.builder()
                        .fileMetadataCache(64L << 20)
                        .listFilesCache(8L << 20, Duration.ofMinutes(5))
                        .fileStatisticsCache(true)
                        .build())
                .build();
        DataFrame df = ctx.sql("SELECT 1");
        ArrowReader reader = df.collect(allocator)) {
      assertTrue(reader.loadNextBatch());
      assertEquals(1, reader.getVectorSchemaRoot().getRowCount());
    }
  }

  @Test
  void emptyCacheManagerOptionsIsHarmlessNoop() {
    // builder().build() with nothing set is on the wire as "all three slots
    // unset". The Rust side treats that as "skip with_cache_manager
    // entirely" and the context construction succeeds.
    try (SessionContext ctx =
        SessionContext.builder().cacheManager(CacheManagerOptions.builder().build()).build()) {
      assertNotNull(ctx);
    }
  }

  @Test
  void fileMetadataCacheZeroIsHarmless() {
    // Upstream allows a 0-byte cap (cache constructed but evicts every
    // insert). Java should not reject it.
    try (SessionContext ctx =
        SessionContext.builder()
            .cacheManager(CacheManagerOptions.builder().fileMetadataCache(0).build())
            .build()) {
      assertNotNull(ctx);
    }
  }
}
