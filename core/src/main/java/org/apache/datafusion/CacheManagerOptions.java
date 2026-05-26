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

import java.time.Duration;

import org.apache.datafusion.protobuf.CacheManagerOptionsProto;
import org.apache.datafusion.protobuf.ListFilesCacheOptionsProto;

/**
 * Configuration for DataFusion's built-in {@code CacheManager}. Pass an instance to {@link
 * SessionContextBuilder#cacheManager(CacheManagerOptions)} to turn on any of the three
 * upstream-provided caches at session construction time.
 *
 * <p>The three caches are independent; calling one setter does not affect the others. A setter that
 * is never called leaves the upstream default in place — so existing callers that don't touch this
 * builder see no behavioral change.
 *
 * <ul>
 *   <li>{@link Builder#fileMetadataCache(long)} — caches file-embedded metadata (e.g. parquet
 *       footers / page metadata). Backed by {@code DefaultFilesMetadataCache} with the supplied
 *       byte cap.
 *   <li>{@link Builder#listFilesCache(long, Duration)} — caches results of object-store {@code
 *       list} operations. Backed by {@code DefaultListFilesCache} with the supplied byte cap and
 *       optional TTL ({@code null} = infinite).
 *   <li>{@link Builder#fileStatisticsCache(boolean)} — caches per-file row counts and column
 *       statistics. Backed by {@code DefaultFileStatisticsCache}; no bytewise cap exists upstream.
 * </ul>
 *
 * <p>This v1 surface configures the <em>built-in</em> cache implementations. Plugging custom Java
 * cache implementations through a JNI upcall path is intentionally out of scope — every cache
 * lookup is a hot path during scans, and routing them through Java would defeat the cache.
 */
public final class CacheManagerOptions {

  private final Long fileMetadataCacheMaxBytes;
  private final ListFilesCacheConfig listFilesCache;
  private final Boolean fileStatisticsCacheEnabled;

  private CacheManagerOptions(Builder b) {
    this.fileMetadataCacheMaxBytes = b.fileMetadataCacheMaxBytes;
    this.listFilesCache = b.listFilesCache;
    this.fileStatisticsCacheEnabled = b.fileStatisticsCacheEnabled;
  }

  /** Begin building a {@link CacheManagerOptions} instance. */
  public static Builder builder() {
    return new Builder();
  }

  CacheManagerOptionsProto toProto() {
    CacheManagerOptionsProto.Builder b = CacheManagerOptionsProto.newBuilder();
    if (fileMetadataCacheMaxBytes != null) {
      b.setFileMetadataCacheMaxBytes(fileMetadataCacheMaxBytes);
    }
    if (listFilesCache != null) {
      ListFilesCacheOptionsProto.Builder lb = ListFilesCacheOptionsProto.newBuilder();
      if (listFilesCache.maxBytes != null) {
        lb.setMaxBytes(listFilesCache.maxBytes);
      }
      if (listFilesCache.ttlMillis != null) {
        lb.setTtlMillis(listFilesCache.ttlMillis);
      }
      b.setListFilesCache(lb.build());
    }
    if (fileStatisticsCacheEnabled != null) {
      b.setFileStatisticsCacheEnabled(fileStatisticsCacheEnabled);
    }
    return b.build();
  }

  /** Internal carrier — `null`-vs-set is the source of truth for "user called this setter". */
  private static final class ListFilesCacheConfig {
    final Long maxBytes;
    final Long ttlMillis;

    ListFilesCacheConfig(Long maxBytes, Long ttlMillis) {
      this.maxBytes = maxBytes;
      this.ttlMillis = ttlMillis;
    }
  }

  /** Builder for {@link CacheManagerOptions}. */
  public static final class Builder {
    private Long fileMetadataCacheMaxBytes;
    private ListFilesCacheConfig listFilesCache;
    private Boolean fileStatisticsCacheEnabled;

    private Builder() {}

    /**
     * Enable the file-embedded metadata cache (parquet footers, page metadata) with the given byte
     * cap. The cap is the budget the upstream {@code DefaultFilesMetadataCache} uses to evict
     * entries; {@code 0} is legal and means "construct the cache but with a 0-byte budget"
     * (effectively disabled but observable in stats).
     *
     * @throws IllegalArgumentException if {@code maxBytes} is negative.
     */
    public Builder fileMetadataCache(long maxBytes) {
      if (maxBytes < 0) {
        throw new IllegalArgumentException(
            "fileMetadataCache maxBytes must be non-negative, got " + maxBytes);
      }
      this.fileMetadataCacheMaxBytes = maxBytes;
      return this;
    }

    /**
     * Enable the list-files cache with the given byte cap and TTL. Pass {@code null} for {@code
     * ttl} to use upstream's "no expiration" semantics (entries are evicted only by capacity
     * pressure).
     *
     * @throws IllegalArgumentException if {@code maxBytes} is negative or {@code ttl} is negative.
     */
    public Builder listFilesCache(long maxBytes, Duration ttl) {
      if (maxBytes < 0) {
        throw new IllegalArgumentException(
            "listFilesCache maxBytes must be non-negative, got " + maxBytes);
      }
      Long ttlMillis = null;
      if (ttl != null) {
        if (ttl.isNegative()) {
          throw new IllegalArgumentException("listFilesCache ttl must be non-negative, got " + ttl);
        }
        ttlMillis = ttl.toMillis();
      }
      this.listFilesCache = new ListFilesCacheConfig(maxBytes, ttlMillis);
      return this;
    }

    /**
     * Enable the file-statistics cache (per-file row counts / column statistics). When {@code
     * enabled} is {@code true}, the upstream {@code DefaultFileStatisticsCache} is installed. When
     * {@code false}, the slot is explicitly set to disabled in the wire format — the same end-state
     * as never calling this setter, but distinguishable for testing.
     */
    public Builder fileStatisticsCache(boolean enabled) {
      this.fileStatisticsCacheEnabled = enabled;
      return this;
    }

    public CacheManagerOptions build() {
      return new CacheManagerOptions(this);
    }
  }
}
