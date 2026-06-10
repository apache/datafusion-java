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

//! Widening cdylib for the generic Spark connector.
//!
//! Single JNI entry point: `wrapWithWidening(jlong) -> jlong`. Takes a raw
//! `FFI_TableProvider` pointer produced by a bridge cdylib, wraps the inner
//! `TableProvider` in a [`WideningTableProvider`] that exposes
//! Spark-compatible Arrow types (UInt*→signed wider, Float16→Float32,
//! Time*→Int wider, Timestamp(*, tz)→Timestamp(Microsecond, tz)), and
//! re-FFIs the result for the consumer (datafusion-java's cdylib).
//!
//! No SessionContext or SQL — kernel-level `arrow::compute::cast` only.

use std::error::Error;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, OnceLock};

use datafusion::catalog::TableProvider;
use datafusion::execution::TaskContextProvider;
use datafusion::prelude::SessionContext;
use datafusion_ffi::execution::FFI_TaskContextProvider;
use datafusion_ffi::table_provider::FFI_TableProvider;
use jni::objects::JClass;
use jni::sys::jlong;
use jni::JNIEnv;
use tokio::runtime::{Handle, Runtime};

pub mod widening;

use widening::WideningTableProvider;

type JniResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// Shared Tokio runtime. The widening cdylib does not itself await any IO,
/// but the FFI_TableProvider it produces is registered on a foreign
/// SessionContext that may schedule work via this handle.
fn runtime() -> &'static Handle {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME
        .get_or_init(|| Runtime::new().expect("tokio runtime init failed"))
        .handle()
}

/// Shared "host" SessionContext within the widening cdylib. Only used as
/// the source of a `TaskContextProvider` passed into `FFI_TableProvider::new`.
/// Lives for the lifetime of the cdylib; no datasets are ever registered on it.
fn host_session_context() -> &'static Arc<SessionContext> {
    static CTX: OnceLock<Arc<SessionContext>> = OnceLock::new();
    CTX.get_or_init(|| Arc::new(SessionContext::new()))
}

#[no_mangle]
pub extern "system" fn Java_io_datafusion_spark_FfiHelperNative_wrapWithWidening<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    ffi_raw_ptr: jlong,
) -> jlong {
    try_unwrap_or_throw(&mut env, 0, |_env| {
        if ffi_raw_ptr == 0 {
            return Err("wrapWithWidening: input FFI_TableProvider pointer is null".into());
        }
        // Take ownership of the producer's FFI_TableProvider.
        let ffi_raw: Box<FFI_TableProvider> =
            unsafe { Box::from_raw(ffi_raw_ptr as *mut FFI_TableProvider) };

        // Cross-cdylib hop: `Arc::<dyn TableProvider>::from(&FFI_TableProvider)`
        // returns a `ForeignTableProvider` wrapper that delegates back through
        // the producer's vtable. Drop our `Box` immediately afterward — the
        // ForeignTableProvider clone owns its own retained copy.
        let inner: Arc<dyn TableProvider> = (&*ffi_raw).into();
        drop(ffi_raw);

        let widened: Arc<dyn TableProvider> = Arc::new(WideningTableProvider::new(inner));

        // Re-wrap as an FFI_TableProvider for the consumer.
        let ctx_provider: Arc<dyn TaskContextProvider> =
            Arc::clone(host_session_context()) as Arc<dyn TaskContextProvider>;
        let ffi_task_ctx = FFI_TaskContextProvider::from(&ctx_provider);
        let ffi = FFI_TableProvider::new(
            widened,
            /*can_support_pushdown_filters=*/ true,
            Some(runtime().clone()),
            ffi_task_ctx,
            /*logical_codec=*/ None,
        );
        Ok(Box::into_raw(Box::new(ffi)) as jlong)
    })
}

/// Run `f`, catching panics and translating `Err` into a plain Java
/// `RuntimeException`. The connector-core helper does not know about
/// datafusion-java's exception hierarchy, so this stays minimal.
fn try_unwrap_or_throw<T, F>(env: &mut JNIEnv, default: T, f: F) -> T
where
    F: FnOnce(&mut JNIEnv) -> JniResult<T>,
{
    match catch_unwind(AssertUnwindSafe(|| f(env))) {
        Ok(Ok(value)) => value,
        Ok(Err(err)) => {
            let _ = env.throw_new("java/lang/RuntimeException", err.to_string());
            default
        }
        Err(panic) => {
            let msg = panic
                .downcast_ref::<&'static str>()
                .map(|s| s.to_string())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "rust panic in widening cdylib".to_string());
            let _ = env.throw_new("java/lang/RuntimeException", format!("panic: {msg}"));
            default
        }
    }
}
