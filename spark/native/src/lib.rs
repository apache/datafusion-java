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

//! Native side of the generic Spark connector.
//!
//! Takes raw `FFI_TableProvider` pointers produced by a bridge cdylib and
//! does everything DataFusion-side in process: schema probe, widening to
//! Spark-compatible Arrow types (UInt*→signed wider, Float16→Float32,
//! Time*→Int wider, Timestamp(*, tz)→Timestamp(Microsecond, tz)), session
//! construction from the driver-pinned config, projection + proto-filter
//! application, planning, and per-partition stream execution. See [`scan`]
//! for the JNI surface and [`widening`] for the cast layer.

use tokio::runtime::Handle;

pub mod scan;
pub mod widening;

/// Shared Tokio runtime (the per-cdylib singleton from
/// `datafusion-jni-common`). Planning and stream execution all run on it.
fn runtime() -> &'static Handle {
    datafusion_jni_common::runtime().handle()
}
