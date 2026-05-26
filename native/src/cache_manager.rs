// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Translate the [`CacheManagerOptionsProto`] message into a
//! [`CacheManagerConfig`] for [`RuntimeEnvBuilder::with_cache_manager`].
//!
//! Each of the three caches is independent; an unset proto field leaves the
//! corresponding upstream default in place (no cache for list-files / stats,
//! a `DefaultFilesMetadataCache` with the default limit for file metadata).
//! When a setter *is* present, the JNI layer always installs a fresh
//! `Default*Cache` impl -- the v1 contract is "configure the built-in
//! caches", not "swap in a custom one".

use std::sync::Arc;
use std::time::Duration;

use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::execution::cache::cache_unit::{
    DefaultFileStatisticsCache, DefaultFilesMetadataCache,
};
use datafusion::execution::cache::DefaultListFilesCache;

use crate::errors::JniResult;
use crate::proto_gen::CacheManagerOptionsProto;

/// Build a [`CacheManagerConfig`] from the proto. Returns `Ok(None)` if the
/// caller did not set any cache-manager field, so the JNI layer can skip the
/// `with_cache_manager(...)` call entirely and let upstream's own
/// `RuntimeEnvBuilder` defaults apply.
pub(crate) fn build_config(
    opts: &CacheManagerOptionsProto,
) -> JniResult<Option<CacheManagerConfig>> {
    // Treat "all three slots unset" as "caller didn't pass a cache_manager
    // message at all" -- avoids overriding upstream's cache-manager-default
    // with a freshly-constructed-but-empty config that happens to be
    // equivalent today but might diverge on a future DataFusion release.
    if opts.file_metadata_cache_max_bytes.is_none()
        && opts.list_files_cache.is_none()
        && opts.file_statistics_cache_enabled.is_none()
    {
        return Ok(None);
    }

    let mut config = CacheManagerConfig::default();

    if let Some(max_bytes) = opts.file_metadata_cache_max_bytes {
        let max = max_bytes as usize;
        config.file_metadata_cache = Some(Arc::new(DefaultFilesMetadataCache::new(max)));
        config.metadata_cache_limit = max;
    }

    if let Some(lfc) = &opts.list_files_cache {
        // `max_bytes` unset → use upstream's documented default
        // (currently 1 MiB; pulled from CacheManagerConfig::default()
        // rather than re-declared here so we track upstream automatically).
        let default_limit = CacheManagerConfig::default().list_files_cache_limit;
        let max = lfc.max_bytes.map(|v| v as usize).unwrap_or(default_limit);
        let ttl = lfc.ttl_millis.map(Duration::from_millis);

        config.list_files_cache = Some(Arc::new(DefaultListFilesCache::new(max, ttl)));
        config.list_files_cache_limit = max;
        config.list_files_cache_ttl = ttl;
    }

    if opts.file_statistics_cache_enabled.unwrap_or(false) {
        config.table_files_statistics_cache = Some(Arc::new(DefaultFileStatisticsCache::default()));
    }

    Ok(Some(config))
}
