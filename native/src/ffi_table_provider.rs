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

//! Generic FFI-bridged TableProvider registration.
//!
//! Accepts a raw `FFI_TableProvider` pointer produced elsewhere — by another
//! cdylib (cross-binary boundary; transparently wrapped via
//! `ForeignTableProvider`) or by Rust code in this same crate (same-binary;
//! library marker lets the impl unwrap to the original Arc).
//!
//! Ownership: the caller's `Box::into_raw(Box::new(FFI_TableProvider))`
//! pointer is consumed here. After this call the pointer must not be reused.

use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion_ffi::table_provider::FFI_TableProvider;
use jni::objects::{JClass, JString};
use jni::sys::jlong;
use jni::JNIEnv;

use crate::errors::{try_unwrap_or_throw, JniResult};

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_registerFfiTableNative<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    name: JString<'local>,
    ffi_ptr: jlong,
) {
    try_unwrap_or_throw(&mut env, (), |env| -> JniResult<()> {
        if handle == 0 {
            return Err("SessionContext handle is null".into());
        }
        if ffi_ptr == 0 {
            return Err("registerFfiTable: FFI_TableProvider pointer is null".into());
        }
        // SAFETY: matches the existing `registerTableNative` pattern — handle
        // came from `createSessionContext` as `Box<SessionContext>` raw ptr.
        let ctx = unsafe { &*(handle as *const SessionContext) };
        let name: String = env.get_string(&name)?.into();

        // Take ownership of the producer's FFI_TableProvider, materialise an
        // Arc<dyn TableProvider> on this side (cross-cdylib hop returns a
        // ForeignTableProvider wrapper; same-cdylib hop returns the original
        // Arc thanks to LIBRARY_MARKER dispatch in datafusion-ffi), then drop
        // the Box — the Arc clone now retains ownership.
        let ffi = unsafe { Box::from_raw(ffi_ptr as *mut FFI_TableProvider) };
        let provider: Arc<dyn TableProvider> = (&*ffi).into();
        drop(ffi);

        ctx.register_table(name.as_str(), provider)?;
        Ok(())
    })
}
