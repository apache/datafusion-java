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

//! Importing providers across a cdylib boundary (the generic FFI path).

use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion_ffi::table_provider::FFI_TableProvider;
use datafusion_jni_common::errors::JniResult;
use jni::sys::jlong;

/// Take ownership of a bridge cdylib's `FFI_TableProvider` pointer and return
/// the in-process provider view. The pointer must be the raw boxed address
/// (`Box::into_raw(Box::new(FFI_TableProvider))`) and must not be reused
/// after this call.
pub fn import_ffi_provider(ffi_raw_ptr: jlong) -> JniResult<Arc<dyn TableProvider>> {
    if ffi_raw_ptr == 0 {
        return Err("FFI_TableProvider pointer is null".into());
    }
    let ffi_raw: Box<FFI_TableProvider> =
        unsafe { Box::from_raw(ffi_raw_ptr as *mut FFI_TableProvider) };
    // `Arc::<dyn TableProvider>::from(&FFI_TableProvider)` returns a
    // ForeignTableProvider that delegates through the producer's vtable; it
    // owns its own retained copy, so our Box can drop immediately.
    let provider: Arc<dyn TableProvider> = (&*ffi_raw).into();
    drop(ffi_raw);
    Ok(provider)
}
