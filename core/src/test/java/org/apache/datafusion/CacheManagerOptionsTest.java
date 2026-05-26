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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;

import org.apache.datafusion.protobuf.CacheManagerOptionsProto;
import org.apache.datafusion.protobuf.SessionOptions;
import org.junit.jupiter.api.Test;

class CacheManagerOptionsTest {

  @Test
  void allThreeCachesRoundTripThroughProto() throws Exception {
    byte[] bytes =
        SessionContext.builder()
            .cacheManager(
                CacheManagerOptions.builder()
                    .fileMetadataCache(64L << 20)
                    .listFilesCache(8L << 20, Duration.ofMinutes(5))
                    .fileStatisticsCache(true)
                    .build())
            .toBytes();

    SessionOptions parsed = SessionOptions.parseFrom(bytes);
    assertTrue(parsed.hasCacheManager());
    CacheManagerOptionsProto cm = parsed.getCacheManager();

    assertTrue(cm.hasFileMetadataCacheMaxBytes());
    assertEquals(64L << 20, cm.getFileMetadataCacheMaxBytes());

    assertTrue(cm.hasListFilesCache());
    assertTrue(cm.getListFilesCache().hasMaxBytes());
    assertEquals(8L << 20, cm.getListFilesCache().getMaxBytes());
    assertTrue(cm.getListFilesCache().hasTtlMillis());
    assertEquals(Duration.ofMinutes(5).toMillis(), cm.getListFilesCache().getTtlMillis());

    assertTrue(cm.hasFileStatisticsCacheEnabled());
    assertTrue(cm.getFileStatisticsCacheEnabled());
  }

  @Test
  void unsetSettersAreAbsentInProto() throws Exception {
    byte[] bytes =
        SessionContext.builder()
            .cacheManager(CacheManagerOptions.builder().fileMetadataCache(64L << 20).build())
            .toBytes();
    SessionOptions parsed = SessionOptions.parseFrom(bytes);
    CacheManagerOptionsProto cm = parsed.getCacheManager();

    assertTrue(cm.hasFileMetadataCacheMaxBytes());
    // The other two travel as unset, not as zero/empty -- the Rust side
    // distinguishes "leave upstream default in place" from "explicitly off".
    assertFalse(cm.hasListFilesCache());
    assertFalse(cm.hasFileStatisticsCacheEnabled());
  }

  @Test
  void listFilesCacheNullTtlIsUnsetOnTheWire() throws Exception {
    byte[] bytes =
        SessionContext.builder()
            .cacheManager(CacheManagerOptions.builder().listFilesCache(8L << 20, null).build())
            .toBytes();
    SessionOptions parsed = SessionOptions.parseFrom(bytes);
    CacheManagerOptionsProto cm = parsed.getCacheManager();
    assertTrue(cm.hasListFilesCache());
    assertTrue(cm.getListFilesCache().hasMaxBytes());
    assertEquals(8L << 20, cm.getListFilesCache().getMaxBytes());
    // ttl_millis is the channel for "infinite" (None on the Rust side) --
    // it has to travel as unset, not as 0.
    assertFalse(cm.getListFilesCache().hasTtlMillis());
  }

  @Test
  void fileMetadataCacheZeroIsAccepted() throws Exception {
    // Upstream allows 0 (the cache is constructed with a 0-byte budget).
    // Java mirrors that contract.
    byte[] bytes =
        SessionContext.builder()
            .cacheManager(CacheManagerOptions.builder().fileMetadataCache(0).build())
            .toBytes();
    SessionOptions parsed = SessionOptions.parseFrom(bytes);
    CacheManagerOptionsProto cm = parsed.getCacheManager();
    assertTrue(cm.hasFileMetadataCacheMaxBytes());
    assertEquals(0L, cm.getFileMetadataCacheMaxBytes());
  }

  @Test
  void fileStatisticsCacheFalseTravelsExplicitlyOnTheWire() throws Exception {
    // `fileStatisticsCache(false)` is a different end-state from "never
    // called this setter" only on the wire -- the Rust side treats both as
    // "leave the slot None" today, but distinguishing the two on the wire
    // lets us assert the bool actually round-tripped.
    byte[] bytes =
        SessionContext.builder()
            .cacheManager(CacheManagerOptions.builder().fileStatisticsCache(false).build())
            .toBytes();
    SessionOptions parsed = SessionOptions.parseFrom(bytes);
    CacheManagerOptionsProto cm = parsed.getCacheManager();
    assertTrue(cm.hasFileStatisticsCacheEnabled());
    assertFalse(cm.getFileStatisticsCacheEnabled());
  }

  @Test
  void unsetCacheManagerIsAbsentInProto() throws Exception {
    // Sanity check that builders that never call .cacheManager(...) emit no
    // cache_manager field at all -- the existing-callers-see-no-change
    // contract relies on this.
    byte[] bytes = SessionContext.builder().batchSize(8192).toBytes();
    SessionOptions parsed = SessionOptions.parseFrom(bytes);
    assertFalse(parsed.hasCacheManager());
  }

  @Test
  void rejectsNegativeFileMetadataCacheMaxBytes() {
    CacheManagerOptions.Builder b = CacheManagerOptions.builder();
    assertThrows(IllegalArgumentException.class, () -> b.fileMetadataCache(-1));
  }

  @Test
  void rejectsNegativeListFilesCacheMaxBytes() {
    CacheManagerOptions.Builder b = CacheManagerOptions.builder();
    assertThrows(IllegalArgumentException.class, () -> b.listFilesCache(-1, null));
  }

  @Test
  void rejectsNegativeListFilesCacheTtl() {
    CacheManagerOptions.Builder b = CacheManagerOptions.builder();
    assertThrows(IllegalArgumentException.class, () -> b.listFilesCache(0, Duration.ofMillis(-1)));
  }

  @Test
  void cacheManagerRejectsNull() {
    SessionContextBuilder b = SessionContext.builder();
    assertThrows(IllegalArgumentException.class, () -> b.cacheManager(null));
  }

  @Test
  void cacheManagerSetterReplacesPreviousValue() throws Exception {
    // Setter replaces; doesn't merge. Calling fileMetadataCache(64) then
    // listFilesCache(8) on a fresh builder must NOT carry the file-metadata
    // setting forward.
    byte[] bytes =
        SessionContext.builder()
            .cacheManager(CacheManagerOptions.builder().fileMetadataCache(64L << 20).build())
            .cacheManager(CacheManagerOptions.builder().listFilesCache(8L << 20, null).build())
            .toBytes();
    SessionOptions parsed = SessionOptions.parseFrom(bytes);
    CacheManagerOptionsProto cm = parsed.getCacheManager();
    // Only list-files survives -- the second .cacheManager(...) call replaced
    // the first.
    assertFalse(cm.hasFileMetadataCacheMaxBytes());
    assertTrue(cm.hasListFilesCache());
  }
}
