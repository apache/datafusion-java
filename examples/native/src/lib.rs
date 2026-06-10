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

//! Example cdylib that produces a small DataFusion `MemTable` wrapped as an
//! `FFI_TableProvider`, returned to the JVM as a `jlong` (the raw boxed
//! pointer). The JVM example uses `SessionContext.registerFfiTable(name, ptr)`
//! to install the provider on a DataFusion session and runs SQL against it.
//!
//! The same pattern is what domain bridges (Rerun, HDF5, custom Iceberg) use
//! to expose their TableProviders to DataFusion-Java — and, transitively, to
//! Spark via the connector-core DataSource V2 plumbing.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;
use datafusion::execution::TaskContextProvider;
use datafusion::prelude::SessionContext;
use datafusion_ffi::execution::FFI_TaskContextProvider;
use datafusion_ffi::table_provider::FFI_TableProvider;
use jni::objects::JClass;
use jni::sys::jlong;
use jni::JNIEnv;
use tokio::runtime::{Handle, Runtime};

/// Tokio runtime that the FFI provider is anchored to. Shared across calls
/// for the lifetime of the cdylib so successive `createMemTableProvider`
/// invocations don't spawn fresh runtimes.
fn runtime() -> &'static Handle {
    use std::sync::OnceLock;
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| Runtime::new().expect("tokio runtime init failed"))
        .handle()
}

/// Throwaway `SessionContext` used only to obtain a `TaskContextProvider`
/// for `FFI_TableProvider::new`. The example does not register anything on it.
fn host_session_context() -> &'static Arc<SessionContext> {
    use std::sync::OnceLock;
    static CTX: OnceLock<Arc<SessionContext>> = OnceLock::new();
    CTX.get_or_init(|| Arc::new(SessionContext::new()))
}

/// Build the example schema + a single-batch in-memory table.
fn build_mem_table() -> Result<Arc<MemTable>, Box<dyn std::error::Error + Send + Sync>> {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]));

    let ids = Int64Array::from(vec![1, 2, 3, 4]);
    let names = StringArray::from(vec![Some("alice"), Some("bob"), None, Some("dave")]);
    let values = Float64Array::from(vec![Some(1.5), Some(2.5), Some(3.5), None]);

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(ids), Arc::new(names), Arc::new(values)],
    )?;

    Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
}

/// JNI entry point: build a small `MemTable`, wrap it in an `FFI_TableProvider`,
/// return the raw boxed pointer as a `jlong`. Ownership of the boxed FFI
/// transfers to the caller — the matching `Box::from_raw` is performed by
/// `SessionContext.registerFfiTable` on the consumer side.
#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_examples_FfiTableProviderExampleNative_createMemTableProvider<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jlong {
    let result: Result<jlong, Box<dyn std::error::Error + Send + Sync>> = (|| {
        let mem_table = build_mem_table()?;
        let provider: Arc<dyn TableProvider> = mem_table;

        let ctx_provider: Arc<dyn TaskContextProvider> =
            Arc::clone(host_session_context()) as Arc<dyn TaskContextProvider>;
        let ffi_task_ctx = FFI_TaskContextProvider::from(&ctx_provider);
        let ffi = FFI_TableProvider::new(
            provider,
            /*can_support_pushdown_filters=*/ true,
            Some(runtime().clone()),
            ffi_task_ctx,
            /*logical_codec=*/ None,
        );
        Ok(Box::into_raw(Box::new(ffi)) as jlong)
    })();

    match result {
        Ok(ptr) => ptr,
        Err(err) => {
            let _ = env.throw_new("java/lang/RuntimeException", err.to_string());
            0
        }
    }
}

/// Drop a previously-created FFI_TableProvider whose pointer was NOT handed
/// off to `registerFfiTable`. Exposed for symmetry — callers that pass the
/// pointer to `registerFfiTable` must NOT also call this; ownership has
/// already transferred.
#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_examples_FfiTableProviderExampleNative_dropProvider<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    ffi_ptr: jlong,
) {
    if ffi_ptr != 0 {
        unsafe {
            drop(Box::from_raw(ffi_ptr as *mut FFI_TableProvider));
        }
    }
}
