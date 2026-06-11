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

mod arrow;
mod avro;
mod cache_manager;
mod csv;
mod jni_util;
mod json;
mod memory;
mod object_store;
mod proto;
mod runtime_metrics;
mod schema;
mod table_provider;
mod udf;

pub(crate) mod proto_gen {
    include!(concat!(env!("OUT_DIR"), "/datafusion_java.rs"));
}

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ffi_stream::FFI_ArrowArrayStream;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatchIterator;
use datafusion::common::{JoinType, UnnestOptions};
use datafusion::config::TableParquetOptions;
use datafusion::dataframe::DataFrame;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::error::DataFusionError;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::{col, Partitioning, ScalarUDF, Signature, SortExpr};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use jni::objects::{JBooleanArray, JByteArray, JClass, JObject, JObjectArray, JString};
use jni::sys::{jboolean, jbyte, jbyteArray, jint, jlong};
use jni::JNIEnv;
use jni::JavaVM;
use prost::Message;
use tokio::runtime::Runtime;

use datafusion_jni_common::errors::{try_unwrap_or_throw, JniResult};
// Re-exported so sibling modules keep their crate-local `crate::StreamingReader` path.
pub(crate) use datafusion_jni_common::StreamingReader;

use crate::proto_gen::ParquetReadOptionsProto;
use crate::proto_gen::SessionOptions;
use crate::schema::decode_optional_schema;

static JAVA_VM: OnceLock<JavaVM> = OnceLock::new();

#[no_mangle]
pub extern "system" fn JNI_OnLoad(vm: JavaVM, _reserved: *mut std::ffi::c_void) -> jni::sys::jint {
    let _ = JAVA_VM.set(vm);
    jni::sys::JNI_VERSION_1_8
}

#[allow(dead_code)]
pub(crate) fn jvm() -> &'static JavaVM {
    JAVA_VM
        .get()
        .expect("JNI_OnLoad has not been called; JavaVM unavailable")
}

pub(crate) fn runtime() -> &'static Runtime {
    // The singleton itself lives in datafusion-jni-common (shared with the
    // Spark helper cdylib; each cdylib statically links its own copy, so the
    // runtime stays per-library). The init hook eagerly installs the
    // runtime-metrics accumulator (no-op when the `runtime-metrics` Cargo
    // feature is off). Initialising here -- not lazily on the first
    // `runtimeStats()` call -- means the RuntimeMonitor's sampling baseline
    // coincides with runtime start, so poll/park/busy totals reflect activity
    // from the first query onward rather than from the first observation.
    datafusion_jni_common::runtime_with_init(crate::runtime_metrics::init)
}

/// Wrap the (already-built) `RuntimeEnvBuilder`'s memory pool with a
/// `TrackingMemoryPool` so `Java_..._memoryUsage` can report current/peak
/// bytes for this session. Registers the tracker in the process-wide map
/// keyed by the JNI handle. Idempotent: if the builder didn't set a pool,
/// fills in DataFusion's default first so the wrap-and-register has the
/// same behavior either way.
fn install_memory_tracker(
    builder: &mut RuntimeEnvBuilder,
) -> std::sync::Arc<crate::memory::TrackingMemoryPool> {
    use datafusion::execution::memory_pool::UnboundedMemoryPool;
    let inner: std::sync::Arc<dyn datafusion::execution::memory_pool::MemoryPool> = builder
        .memory_pool
        .take()
        .unwrap_or_else(|| std::sync::Arc::new(UnboundedMemoryPool::default()));
    let tracker = std::sync::Arc::new(crate::memory::TrackingMemoryPool::new(inner));
    builder.memory_pool = Some(tracker.clone());
    tracker
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_createSessionContext<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jlong {
    try_unwrap_or_throw(&mut env, 0, |_env| -> JniResult<jlong> {
        let mut runtime_builder = RuntimeEnvBuilder::new();
        let tracker = install_memory_tracker(&mut runtime_builder);
        let runtime_env = runtime_builder.build()?;
        let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), Arc::new(runtime_env));
        let handle = Box::into_raw(Box::new(ctx)) as jlong;
        crate::memory::register(handle, tracker);
        Ok(handle)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_createSessionContextWithOptions<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    options_bytes: JByteArray<'local>,
) -> jlong {
    try_unwrap_or_throw(&mut env, 0, |env| -> JniResult<jlong> {
        let bytes: Vec<u8> = env.convert_byte_array(&options_bytes)?;
        let opts = SessionOptions::decode(bytes.as_slice())?;

        let mut config = SessionConfig::new();
        if let Some(v) = opts.batch_size {
            config = config.with_batch_size(v as usize);
        }
        if let Some(v) = opts.target_partitions {
            config = config.with_target_partitions(v as usize);
        }
        if let Some(v) = opts.collect_statistics {
            config = config.with_collect_statistics(v);
        }
        if let Some(v) = opts.information_schema {
            config = config.with_information_schema(v);
        }

        let mut runtime_builder = RuntimeEnvBuilder::new();
        if let Some(mem) = opts.memory_limit {
            runtime_builder = runtime_builder
                .with_memory_limit(mem.max_memory_bytes as usize, mem.memory_fraction);
        }
        if let Some(dir) = opts.temp_directory {
            runtime_builder = runtime_builder.with_temp_file_path(PathBuf::from(dir));
        }
        // disk_manager carries the disable / size-cap surface added on top of
        // the legacy temp_directory field. Java-side builder enforces that
        // disabled and tempDirectory aren't both set; the Rust layer doesn't
        // re-validate because there's no path that produces a contradictory
        // mode here -- with_disk_manager_builder(Disabled) wholesale
        // replaces any prior with_temp_file_path call, and that's the
        // semantics callers using the typed Java setters can already see.
        if let Some(dm) = opts.disk_manager.as_ref() {
            if dm.disabled() {
                let builder = DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled);
                runtime_builder = runtime_builder.with_disk_manager_builder(builder);
            }
            if let Some(size) = dm.max_temp_directory_size {
                runtime_builder = runtime_builder.with_max_temp_directory_size(size);
            }
        }

        if let Some(cm_opts) = opts.cache_manager.as_ref() {
            if let Some(cm_config) = crate::cache_manager::build_config(cm_opts)? {
                runtime_builder = runtime_builder.with_cache_manager(cm_config);
            }
        }

        // datafusion.runtime.* keys live on RuntimeEnv (separate object from
        // SessionConfig) and round-tripping them through getOption/setOption
        // has subtle correctness pitfalls (lazy default-tempdir creation,
        // upstream's K/M/G integer truncation, OS-specific path separators).
        // Reject them here with a clear error so callers fall back to the
        // typed memoryLimit() / tempDirectory() setters until a follow-up
        // PR designs the side-cache needed to support them safely.
        //
        // Iteration order matters: some upstream setters have side effects on
        // other keys (e.g. `datafusion.optimizer.enable_dynamic_filter_pushdown`
        // also rewrites the per-operator `enable_*_dynamic_filter_pushdown`
        // flags), so the caller's last write must win. The proto field is
        // `repeated ConfigOption` for this reason -- prost's default
        // `map<string,string>` decodes to a HashMap whose iteration order is
        // randomized.
        for opt in &opts.options {
            if opt.key.starts_with("datafusion.runtime.") {
                return Err(format!(
                    "datafusion.runtime.* keys are not supported via setOption yet; \
                     use SessionContextBuilder.memoryLimit() / .tempDirectory() instead. \
                     Got: {} = {}",
                    opt.key, opt.value
                )
                .into());
            }
            config.options_mut().set(&opt.key, &opt.value)?;
        }

        // Wrap the configured pool (default `UnboundedMemoryPool` or whatever
        // `with_memory_limit` produced) with a tracker so `memoryUsage()` can
        // report current/peak bytes for this session. The tracker is
        // transparent to query execution -- it only intercepts grow/shrink to
        // update two atomics.
        let tracker = install_memory_tracker(&mut runtime_builder);

        let runtime_env = runtime_builder.build()?;
        let ctx = SessionContext::new_with_config_rt(config, Arc::new(runtime_env));

        // Object-store registrations come last because they need a built
        // RuntimeEnv to register against. A failure here drops `ctx` (and its
        // Arc<RuntimeEnv>) on the floor and surfaces as a Java RuntimeException
        // via try_unwrap_or_throw.
        crate::object_store::apply_registrations(&ctx, &opts.object_stores)?;

        let handle = Box::into_raw(Box::new(ctx)) as jlong;
        crate::memory::register(handle, tracker);
        Ok(handle)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_createDataFrame<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    sql: JString<'local>,
) -> jlong {
    try_unwrap_or_throw(&mut env, 0, |env| -> JniResult<jlong> {
        if handle == 0 {
            return Err("SessionContext handle is null".into());
        }
        let ctx = unsafe { &*(handle as *const SessionContext) };
        let sql_str: String = env.get_string(&sql)?.into();

        let df = runtime().block_on(async { ctx.sql(&sql_str).await })?;
        Ok(Box::into_raw(Box::new(df)) as jlong)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_DataFrame_collectDataFrame<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    ffi_stream_addr: jlong,
) {
    try_unwrap_or_throw(&mut env, (), |_env| -> JniResult<()> {
        if handle == 0 {
            return Err("DataFrame handle is null".into());
        }
        if ffi_stream_addr == 0 {
            return Err("ffi stream address is null".into());
        }
        let df = unsafe { *Box::from_raw(handle as *mut DataFrame) };

        let ffi: FFI_ArrowArrayStream = runtime().block_on(async {
            let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
            let batches = df.collect().await?;
            let iter = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
            Ok::<_, DataFusionError>(FFI_ArrowArrayStream::new(Box::new(iter)))
        })?;

        unsafe {
            std::ptr::write(ffi_stream_addr as *mut FFI_ArrowArrayStream, ffi);
        }
        Ok(())
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_DataFrame_executeStreamDataFrame<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    ffi_stream_addr: jlong,
) {
    try_unwrap_or_throw(&mut env, (), |_env| -> JniResult<()> {
        if handle == 0 {
            return Err("DataFrame handle is null".into());
        }
        if ffi_stream_addr == 0 {
            return Err("ffi stream address is null".into());
        }
        let df = unsafe { *Box::from_raw(handle as *mut DataFrame) };

        let ffi: FFI_ArrowArrayStream = runtime().block_on(async {
            let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
            let stream = df.execute_stream().await?;
            let reader = StreamingReader { schema, stream };
            Ok::<_, DataFusionError>(FFI_ArrowArrayStream::new(Box::new(reader)))
        })?;

        unsafe {
            std::ptr::write(ffi_stream_addr as *mut FFI_ArrowArrayStream, ffi);
        }
        Ok(())
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_DataFrame_countRows<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    try_unwrap_or_throw(&mut env, 0, |_env| -> JniResult<jlong> {
        if handle == 0 {
            return Err("DataFrame handle is null".into());
        }
        let df = unsafe { &*(handle as *const DataFrame) }.clone();
        let n = runtime().block_on(async { df.count().await })?;
        Ok(n as jlong)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_DataFrame_schemaIpc<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    try_unwrap_or_throw(
        &mut env,
        std::ptr::null_mut(),
        |env| -> JniResult<jbyteArray> {
            if handle == 0 {
                return Err("DataFrame handle is null".into());
            }
            let df = unsafe { &*(handle as *const DataFrame) };
            let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());

            let mut buf: Vec<u8> = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut buf, schema.as_ref())?;
                writer.finish()?;
            }
            let arr = env.byte_array_from_slice(&buf)?;
            Ok(arr.into_raw())
        },
    )
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_DataFrame_explainPlan<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    verbose: jboolean,
    analyze: jboolean,
) -> jlong {
    try_unwrap_or_throw(&mut env, 0, |_env| -> JniResult<jlong> {
        if handle == 0 {
            return Err("DataFrame handle is null".into());
        }
        let df = unsafe { &*(handle as *const DataFrame) }.clone();
        let new_df = df.explain(verbose != 0, analyze != 0)?;
        Ok(Box::into_raw(Box::new(new_df)) as jlong)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_DataFrame_cachePlan<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    try_unwrap_or_throw(&mut env, 0, |_env| -> JniResult<jlong> {
        if handle == 0 {
            return Err("DataFrame handle is null".into());
        }
        let df = unsafe { &*(handle as *const DataFrame) }.clone();
        let new_df = runtime().block_on(async { df.cache().await })?;
        Ok(Box::into_raw(Box::new(new_df)) as jlong)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_DataFrame_describePlan<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    try_unwrap_or_throw(&mut env, 0, |_env| -> JniResult<jlong> {
        if handle == 0 {
            return Err("DataFrame handle is null".into());
        }
        let df = unsafe { &*(handle as *const DataFrame) }.clone();
        let new_df = runtime().block_on(async { df.describe().await })?;
        Ok(Box::into_raw(Box::new(new_df)) as jlong)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_DataFrame_showDataFrame<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    try_unwrap_or_throw(&mut env, (), |_env| -> JniResult<()> {
        if handle == 0 {
            return Err("DataFrame handle is null".into());
        }
        let df = unsafe { &*(handle as *const DataFrame) }.clone();
        runtime().block_on(async { df.show().await })?;
        Ok(())
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_DataFrame_showDataFrameWithLimit<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    limit: jint,
) {
    try_unwrap_or_throw(&mut env, (), |_env| -> JniResult<()> {
        if handle == 0 {
            return Err("DataFrame handle is null".into());
        }
        let df = unsafe { &*(handle as *const DataFrame) }.clone();
        runtime().block_on(async { df.show_limit(limit as usize).await })?;
        Ok(())
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_DataFrame_selectColumns<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    column_names: JObjectArray<'local>,
) -> jlong {
    try_unwrap_or_throw(&mut env, 0, |env| -> JniResult<jlong> {
        if handle == 0 {
            return Err("DataFrame handle is null".into());
        }
        let df = unsafe { &*(handle as *const DataFrame) }.clone();

        let len = env.get_array_length(&column_names)?;
        let mut owned: Vec<String> = Vec::with_capacity(len as usize);
        for i in 0..len {
            let elem = env.get_object_array_element(&column_names, i)?;
            let jstr: JString = elem.into();
            owned.push(env.get_string(&jstr)?.into());
        }
        let refs: Vec<&str> = owned.iter().map(String::as_str).collect();

        let new_df = df.select_columns(&refs)?;
        Ok(Box::into_raw(Box::new(new_df)) as jlong)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_DataFrame_filterRows<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    predicate: JString<'local>,
) -> jlong {
    try_unwrap_or_throw(&mut env, 0, |env| -> JniResult<jlong> {
        if handle == 0 {
            return Err("DataFrame handle is null".into());
        }
        let df = unsafe { &*(handle as *const DataFrame) }.clone();
        let predicate: String = env.get_string(&predicate)?.into();
        let expr = df.parse_sql_expr(&predicate)?;
        let new_df = df.filter(expr)?;
        Ok(Box::into_raw(Box::new(new_df)) as jlong)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_DataFrame_limitRows<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    skip: jint,
    fetch: jint,
) -> jlong {
    try_unwrap_or_throw(&mut env, 0, |_env| -> JniResult<jlong> {
        if handle == 0 {
            return Err("DataFrame handle is null".into());
        }
        let df = unsafe { &*(handle as *const DataFrame) }.clone();
        let new_df = df.limit(skip as usize, Some(fetch as usize))?;
        Ok(Box::into_raw(Box::new(new_df)) as jlong)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_DataFrame_distinctRows<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    try_unwrap_or_throw(&mut env, 0, |_env| -> JniResult<jlong> {
        if handle == 0 {
            return Err("DataFrame handle is null".into());
        }
        let df = unsafe { &*(handle as *const DataFrame) }.clone();
        let new_df = df.distinct()?;
        Ok(Box::into_raw(Box::new(new_df)) as jlong)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_DataFrame_dropColumns<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    column_names: JObjectArray<'local>,
) -> jlong {
    try_unwrap_or_throw(&mut env, 0, |env| -> JniResult<jlong> {
        if handle == 0 {
            return Err("DataFrame handle is null".into());
        }
        let df = unsafe { &*(handle as *const DataFrame) }.clone();

        let len = env.get_array_length(&column_names)?;
        let mut owned: Vec<String> = Vec::with_capacity(len as usize);
        for i in 0..len {
            let elem = env.get_object_array_element(&column_names, i)?;
            let jstr: JString = elem.into();
            owned.push(env.get_string(&jstr)?.into());
        }
        let refs: Vec<&str> = owned.iter().map(String::as_str).collect();

        let new_df = df.drop_columns(&refs)?;
        Ok(Box::into_raw(Box::new(new_df)) as jlong)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_DataFrame_renameColumn<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    old_name: JString<'local>,
    new_name: JString<'local>,
) -> jlong {
    try_unwrap_or_throw(&mut env, 0, |env| -> JniResult<jlong> {
        if handle == 0 {
            return Err("DataFrame handle is null".into());
        }
        let df = unsafe { &*(handle as *const DataFrame) }.clone();
        let old: String = env.get_string(&old_name)?.into();
        let new: String = env.get_string(&new_name)?.into();
        let new_df = df.with_column_renamed(&old, &new)?;
        Ok(Box::into_raw(Box::new(new_df)) as jlong)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_DataFrame_withColumnExpr<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    name: JString<'local>,
    expr: JString<'local>,
) -> jlong {
    try_unwrap_or_throw(&mut env, 0, |env| -> JniResult<jlong> {
        if handle == 0 {
            return Err("DataFrame handle is null".into());
        }
        let df = unsafe { &*(handle as *const DataFrame) }.clone();
        let name: String = env.get_string(&name)?.into();
        let expr: String = env.get_string(&expr)?.into();
        let parsed = df.parse_sql_expr(&expr)?;
        let new_df = df.with_column(&name, parsed)?;
        Ok(Box::into_raw(Box::new(new_df)) as jlong)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_DataFrame_unnestColumns<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    columns: JObjectArray<'local>,
    preserve_nulls: jboolean,
) -> jlong {
    try_unwrap_or_throw(&mut env, 0, |env| -> JniResult<jlong> {
        if handle == 0 {
            return Err("DataFrame handle is null".into());
        }
        let df = unsafe { &*(handle as *const DataFrame) }.clone();

        let len = env.get_array_length(&columns)?;
        let mut owned: Vec<String> = Vec::with_capacity(len as usize);
        for i in 0..len {
            let elem = env.get_object_array_element(&columns, i)?;
            let jstr: JString = elem.into();
            owned.push(env.get_string(&jstr)?.into());
        }
        let refs: Vec<&str> = owned.iter().map(String::as_str).collect();

        let opts = UnnestOptions::new().with_preserve_nulls(preserve_nulls != 0);
        let new_df = df.unnest_columns_with_options(&refs, opts)?;
        Ok(Box::into_raw(Box::new(new_df)) as jlong)
    })
}

// -- Set operations -------------------------------------------------------
//
// Each handler clones both DataFrames -- DataFusion's set-op methods consume
// `self` and the argument by value, but `DataFrame: Clone` is cheap (the
// underlying LogicalPlan is Arc-backed), so cloning lets us keep both Java
// receivers usable. The Java side already validates that both handles are
// non-null before reaching here.
macro_rules! set_op_handler {
    ($fn_name:ident, $df_method:ident) => {
        #[no_mangle]
        pub extern "system" fn $fn_name<'local>(
            mut env: JNIEnv<'local>,
            _class: JClass<'local>,
            handle: jlong,
            other_handle: jlong,
        ) -> jlong {
            try_unwrap_or_throw(&mut env, 0, |_env| -> JniResult<jlong> {
                if handle == 0 {
                    return Err("DataFrame handle is null".into());
                }
                if other_handle == 0 {
                    return Err("other DataFrame handle is null".into());
                }
                let df = unsafe { &*(handle as *const DataFrame) }.clone();
                let other = unsafe { &*(other_handle as *const DataFrame) }.clone();
                let new_df = df.$df_method(other)?;
                Ok(Box::into_raw(Box::new(new_df)) as jlong)
            })
        }
    };
}

set_op_handler!(Java_org_apache_datafusion_DataFrame_unionRows, union);
set_op_handler!(
    Java_org_apache_datafusion_DataFrame_unionDistinctRows,
    union_distinct
);
set_op_handler!(
    Java_org_apache_datafusion_DataFrame_unionByNameRows,
    union_by_name
);
set_op_handler!(
    Java_org_apache_datafusion_DataFrame_unionByNameDistinctRows,
    union_by_name_distinct
);
set_op_handler!(
    Java_org_apache_datafusion_DataFrame_intersectRows,
    intersect
);
set_op_handler!(
    Java_org_apache_datafusion_DataFrame_intersectDistinctRows,
    intersect_distinct
);
set_op_handler!(Java_org_apache_datafusion_DataFrame_exceptRows, except);
set_op_handler!(
    Java_org_apache_datafusion_DataFrame_exceptDistinctRows,
    except_distinct
);

/// Map a Java {@code JoinType.code()} byte back to upstream's enum.
fn join_type_from_byte(byte: u8) -> JniResult<JoinType> {
    match byte {
        0 => Ok(JoinType::Inner),
        1 => Ok(JoinType::Left),
        2 => Ok(JoinType::Right),
        3 => Ok(JoinType::Full),
        4 => Ok(JoinType::LeftSemi),
        5 => Ok(JoinType::RightSemi),
        6 => Ok(JoinType::LeftAnti),
        7 => Ok(JoinType::RightAnti),
        8 => Ok(JoinType::LeftMark),
        9 => Ok(JoinType::RightMark),
        other => Err(format!("unknown join type byte: {other}").into()),
    }
}

/// Build a combined DFSchema for SQL parsing of a join filter or `joinOn` predicate.
/// Mirrors how upstream's `LogicalPlanBuilder::join_detailed` normalises the parsed Expr
/// against `&[&[left_schema, right_schema]]`: tolerate unrelated duplicate-named columns
/// rather than rejecting them via `DFSchema::join`'s `check_names`. `DFSchema::merge`
/// skips duplicates (left side wins for unqualified collisions), which is fine for the
/// SQL-to-Expr step -- the subsequent join planner runs the real ambiguity check.
fn combine_schemas(
    left: &datafusion::common::DFSchema,
    right: &datafusion::common::DFSchema,
) -> datafusion::common::DFSchema {
    let mut combined = left.clone();
    combined.merge(right);
    combined
}

/// Drain a Java {@code String[]} into an owned {@code Vec<String>}.
fn collect_jstring_array(env: &mut JNIEnv, arr: &JObjectArray) -> JniResult<Vec<String>> {
    let len = env.get_array_length(arr)?;
    let mut owned: Vec<String> = Vec::with_capacity(len as usize);
    for i in 0..len {
        let elem = env.get_object_array_element(arr, i)?;
        let jstr: JString = elem.into();
        owned.push(env.get_string(&jstr)?.into());
    }
    Ok(owned)
}

#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_org_apache_datafusion_DataFrame_joinDataFrame<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_handle: jlong,
    right_handle: jlong,
    join_type: jbyte,
    left_cols: JObjectArray<'local>,
    right_cols: JObjectArray<'local>,
    filter: JString<'local>,
) -> jlong {
    try_unwrap_or_throw(&mut env, 0, |env| -> JniResult<jlong> {
        if left_handle == 0 {
            return Err("left DataFrame handle is null".into());
        }
        if right_handle == 0 {
            return Err("right DataFrame handle is null".into());
        }
        let left = unsafe { &*(left_handle as *const DataFrame) }.clone();
        let right = unsafe { &*(right_handle as *const DataFrame) }.clone();
        let join_type = join_type_from_byte(join_type as u8)?;

        let left_owned: Vec<String> = collect_jstring_array(env, &left_cols)?;
        let right_owned: Vec<String> = collect_jstring_array(env, &right_cols)?;
        let left_refs: Vec<&str> = left_owned.iter().map(String::as_str).collect();
        let right_refs: Vec<&str> = right_owned.iter().map(String::as_str).collect();

        // The optional residual filter spans both sides and must be parsed against the
        // combined schema. parse_sql_expr only sees one DataFrame's schema, so reach into
        // the SessionState via into_parts() on a clone. Use DFSchema::merge rather than
        // DFSchema::join so the parser tolerates unrelated duplicate unqualified columns
        // shared by both sides (e.g. both inputs carrying a `created_at` field) -- merge
        // skips the duplicates while join's check_names rejects them. Upstream's join
        // path normalises the parsed Expr against both schemas as a precedence list, so
        // ambiguous references genuinely used in the filter are still surfaced after
        // parsing.
        let filter_expr: Option<Expr> = if filter.is_null() {
            None
        } else {
            let filter_sql: String = env.get_string(&filter)?.into();
            let combined = combine_schemas(left.schema(), right.schema());
            let (state, _plan) = left.clone().into_parts();
            Some(state.create_logical_expr(&filter_sql, &combined)?)
        };

        let new_df = left.join(right, join_type, &left_refs, &right_refs, filter_expr)?;
        Ok(Box::into_raw(Box::new(new_df)) as jlong)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_DataFrame_joinOnDataFrame<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_handle: jlong,
    right_handle: jlong,
    join_type: jbyte,
    predicates: JObjectArray<'local>,
) -> jlong {
    try_unwrap_or_throw(&mut env, 0, |env| -> JniResult<jlong> {
        if left_handle == 0 {
            return Err("left DataFrame handle is null".into());
        }
        if right_handle == 0 {
            return Err("right DataFrame handle is null".into());
        }
        let left = unsafe { &*(left_handle as *const DataFrame) }.clone();
        let right = unsafe { &*(right_handle as *const DataFrame) }.clone();
        let join_type = join_type_from_byte(join_type as u8)?;

        let predicates_owned: Vec<String> = collect_jstring_array(env, &predicates)?;
        // See joinDataFrame for the rationale behind combine_schemas vs DFSchema::join.
        let combined = combine_schemas(left.schema(), right.schema());
        let (state, _plan) = left.clone().into_parts();
        let exprs: Vec<Expr> = predicates_owned
            .iter()
            .map(|sql| state.create_logical_expr(sql, &combined))
            .collect::<datafusion::error::Result<Vec<_>>>()?;

        let new_df = left.join_on(right, join_type, exprs)?;
        Ok(Box::into_raw(Box::new(new_df)) as jlong)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_DataFrame_sortRows<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    columns: JObjectArray<'local>,
    ascending: JBooleanArray<'local>,
    nulls_first: JBooleanArray<'local>,
) -> jlong {
    try_unwrap_or_throw(&mut env, 0, |env| -> JniResult<jlong> {
        if handle == 0 {
            return Err("DataFrame handle is null".into());
        }
        let df = unsafe { &*(handle as *const DataFrame) }.clone();

        let len = env.get_array_length(&columns)? as usize;
        let mut names: Vec<String> = Vec::with_capacity(len);
        for i in 0..len {
            let elem = env.get_object_array_element(&columns, i as i32)?;
            let jstr: JString = elem.into();
            names.push(env.get_string(&jstr)?.into());
        }

        // Decode the two parallel boolean arrays via primitive-array region copies.
        let mut asc_buf = vec![0u8; len];
        env.get_boolean_array_region(&ascending, 0, &mut asc_buf)?;
        let mut nf_buf = vec![0u8; len];
        env.get_boolean_array_region(&nulls_first, 0, &mut nf_buf)?;

        let sort_exprs: Vec<SortExpr> = names
            .into_iter()
            .zip(asc_buf.into_iter().zip(nf_buf))
            .map(|(name, (asc, nf))| SortExpr::new(col(&name), asc != 0, nf != 0))
            .collect();

        let new_df = df.sort(sort_exprs)?;
        Ok(Box::into_raw(Box::new(new_df)) as jlong)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_DataFrame_repartitionRoundRobinRows<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    num_partitions: jint,
) -> jlong {
    try_unwrap_or_throw(&mut env, 0, |_env| -> JniResult<jlong> {
        if handle == 0 {
            return Err("DataFrame handle is null".into());
        }
        let df = unsafe { &*(handle as *const DataFrame) }.clone();
        let new_df = df.repartition(Partitioning::RoundRobinBatch(num_partitions as usize))?;
        Ok(Box::into_raw(Box::new(new_df)) as jlong)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_DataFrame_repartitionHashRows<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    num_partitions: jint,
    columns: JObjectArray<'local>,
) -> jlong {
    try_unwrap_or_throw(&mut env, 0, |env| -> JniResult<jlong> {
        if handle == 0 {
            return Err("DataFrame handle is null".into());
        }
        let df = unsafe { &*(handle as *const DataFrame) }.clone();

        let len = env.get_array_length(&columns)?;
        let mut owned: Vec<String> = Vec::with_capacity(len as usize);
        for i in 0..len {
            let elem = env.get_object_array_element(&columns, i)?;
            let jstr: JString = elem.into();
            owned.push(env.get_string(&jstr)?.into());
        }
        let exprs = owned.iter().map(|s| col(s.as_str())).collect();

        let new_df = df.repartition(Partitioning::Hash(exprs, num_partitions as usize))?;
        Ok(Box::into_raw(Box::new(new_df)) as jlong)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_DataFrame_writeParquetWithOptions<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    path: JString<'local>,
    compression: JString<'local>,
    single_file_output_set: jboolean,
    single_file_output_value: jboolean,
) {
    try_unwrap_or_throw(&mut env, (), |env| -> JniResult<()> {
        if handle == 0 {
            return Err("DataFrame handle is null".into());
        }
        let df = unsafe { &*(handle as *const DataFrame) }.clone();
        let path: String = env.get_string(&path)?.into();

        // When the caller left `singleFileOutput` unset, force directory output (`false`)
        // rather than leaving DataFusion in `Automatic` mode. Automatic mode treats paths
        // with an extension (e.g. `out.parquet`) as single-file targets, which would silently
        // contradict the documented "directory unless overridden" default and surprise any
        // caller that hands writeParquet a `.parquet` path.
        let single_file = if single_file_output_set != 0 {
            single_file_output_value != 0
        } else {
            false
        };
        let write_opts = DataFrameWriteOptions::new().with_single_file_output(single_file);

        let writer_opts: Option<TableParquetOptions> = if !compression.is_null() {
            let c: String = env.get_string(&compression)?.into();
            let mut tpo = TableParquetOptions::default();
            tpo.global.compression = Some(c);
            Some(tpo)
        } else {
            None
        };

        runtime().block_on(df.write_parquet(&path, write_opts, writer_opts))?;
        Ok(())
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_DataFrame_closeDataFrame<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    try_unwrap_or_throw(&mut env, (), |_env| -> JniResult<()> {
        if handle != 0 {
            unsafe {
                drop(Box::from_raw(handle as *mut DataFrame));
            }
        }
        Ok(())
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_getOptionNative<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    key: JString<'local>,
) -> jni::sys::jstring {
    try_unwrap_or_throw(
        &mut env,
        std::ptr::null_mut(),
        |env| -> JniResult<jni::sys::jstring> {
            if handle == 0 {
                return Err("SessionContext handle is null".into());
            }
            let ctx = unsafe { &*(handle as *const SessionContext) };
            let key: String = env.get_string(&key)?.into();

            // datafusion.runtime.* keys live on RuntimeEnv and are not yet
            // supported via this getter (see the matching restriction in
            // createSessionContextWithOptions). Reject them with a clear
            // pointer to the typed alternatives.
            if key.starts_with("datafusion.runtime.") {
                return Err(format!(
                    "datafusion.runtime.* keys are not supported via getOption yet; \
                     use SessionContextBuilder typed setters instead. Got: {key}"
                )
                .into());
            }

            let config = ctx.copied_config();
            for entry in config.options().entries() {
                if entry.key == key {
                    return match entry.value {
                        Some(v) => Ok(env.new_string(v)?.into_raw()),
                        None => Ok(std::ptr::null_mut()),
                    };
                }
            }

            Err(format!("unknown DataFusion config key: {key}").into())
        },
    )
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_closeSessionContext<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    try_unwrap_or_throw(&mut env, (), |_env| -> JniResult<()> {
        if handle != 0 {
            // Drop our side-table entry first so the tracker Arc's last
            // strong reference is the SessionContext->RuntimeEnv chain, then
            // drop the SessionContext itself.
            crate::memory::unregister(handle);
            unsafe {
                drop(Box::from_raw(handle as *mut SessionContext));
            }
        }
        Ok(())
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_memoryUsageNative<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jni::sys::jlongArray {
    try_unwrap_or_throw(
        &mut env,
        std::ptr::null_mut(),
        |env| -> JniResult<jni::sys::jlongArray> {
            if handle == 0 {
                return Err("SessionContext handle is null".into());
            }
            // Look up the tracker by JNI handle. Should always succeed because
            // the ctor inserts before publishing the handle to Java.
            let tracker = crate::memory::lookup(handle)
                .ok_or("memory tracker not registered for this SessionContext")?;
            // `snapshot()` returns a consistent (current, peak) pair where
            // peak is always >= current even if a concurrent record_grow is
            // in-flight. Callers see no transient `current > peak`.
            let (current, peak) = tracker.snapshot();
            let values: [jlong; 2] = [current as jlong, peak as jlong];
            let arr = env.new_long_array(values.len() as jni::sys::jsize)?;
            env.set_long_array_region(&arr, 0, &values)?;
            Ok(arr.into_raw())
        },
    )
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_runtimeStatsNative<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jni::sys::jlongArray {
    try_unwrap_or_throw(
        &mut env,
        std::ptr::null_mut(),
        |env| -> JniResult<jni::sys::jlongArray> {
            if handle == 0 {
                return Err("SessionContext handle is null".into());
            }
            // Runtime stats are process-global -- the JNI library drives one
            // shared multi-threaded runtime -- but we still gate on the
            // SessionContext handle so callers can't ask after close().
            let stats = crate::runtime_metrics::runtime_stats()?;
            let arr = env.new_long_array(stats.len() as jni::sys::jsize)?;
            env.set_long_array_region(&arr, 0, &stats)?;
            Ok(arr.into_raw())
        },
    )
}

fn with_parquet_options<R>(
    env: &mut JNIEnv,
    options_bytes: JByteArray,
    schema_ipc_bytes: JByteArray,
    f: impl FnOnce(ParquetReadOptions) -> JniResult<R>,
) -> JniResult<R> {
    let bytes: Vec<u8> = env.convert_byte_array(&options_bytes)?;
    let p = ParquetReadOptionsProto::decode(bytes.as_slice())?;

    let schema = decode_optional_schema(env, schema_ipc_bytes)?;

    let file_ext = p.file_extension;
    let mut opts = ParquetReadOptions::default().file_extension(&file_ext);
    if let Some(v) = p.parquet_pruning {
        opts = opts.parquet_pruning(v);
    }
    if let Some(v) = p.skip_metadata {
        opts = opts.skip_metadata(v);
    }
    if let Some(v) = p.metadata_size_hint {
        opts = opts.metadata_size_hint(Some(v as usize));
    }
    if let Some(ref s) = schema {
        opts = opts.schema(s);
    }

    f(opts)
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_registerParquetWithOptions<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    name: JString<'local>,
    path: JString<'local>,
    options_bytes: JByteArray<'local>,
    schema_ipc_bytes: JByteArray<'local>,
) {
    try_unwrap_or_throw(&mut env, (), |env| -> JniResult<()> {
        if handle == 0 {
            return Err("SessionContext handle is null".into());
        }
        let ctx = unsafe { &*(handle as *const SessionContext) };
        let name: String = env.get_string(&name)?.into();
        let path: String = env.get_string(&path)?.into();
        with_parquet_options(env, options_bytes, schema_ipc_bytes, |opts| {
            runtime().block_on(async {
                ctx.register_parquet(&name, &path, opts).await?;
                Ok::<(), DataFusionError>(())
            })?;
            Ok(())
        })
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_readParquetWithOptions<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    path: JString<'local>,
    options_bytes: JByteArray<'local>,
    schema_ipc_bytes: JByteArray<'local>,
) -> jlong {
    try_unwrap_or_throw(&mut env, 0, |env| -> JniResult<jlong> {
        if handle == 0 {
            return Err("SessionContext handle is null".into());
        }
        let ctx = unsafe { &*(handle as *const SessionContext) };
        let path: String = env.get_string(&path)?.into();
        with_parquet_options(env, options_bytes, schema_ipc_bytes, |opts| {
            let df = runtime().block_on(ctx.read_parquet(path, opts))?;
            Ok(Box::into_raw(Box::new(df)) as jlong)
        })
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_registerScalarUdf<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    name: JString<'local>,
    signature_schema_bytes: JByteArray<'local>,
    volatility: jni::sys::jbyte,
    udf: JObject<'local>,
) {
    try_unwrap_or_throw(&mut env, (), |env| -> JniResult<()> {
        if handle == 0 {
            return Err("SessionContext handle is null".into());
        }
        // SAFETY: handle is a valid Box<SessionContext> allocated by createSessionContext.
        let ctx = unsafe { &*(handle as *const SessionContext) };
        let name: String = env.get_string(&name)?.into();

        // Decode the signature schema (field 0 = return type, fields 1..N = arg types).
        let signature_schema = crate::schema::decode_optional_schema(env, signature_schema_bytes)?
            .ok_or("signature schema bytes were null")?;
        let fields = signature_schema.fields();
        if fields.is_empty() {
            return Err("signature schema must have at least a return-type field".into());
        }
        let return_field = fields[0].clone();
        let arg_types: Vec<datafusion::arrow::datatypes::DataType> = fields
            .iter()
            .skip(1)
            .map(|f| f.data_type().clone())
            .collect();

        let volatility = crate::udf::volatility_from_byte(volatility as u8)?;
        let signature = Signature::exact(arg_types, volatility);

        // Hold references that survive the JNI call.
        let udf_global_ref = env.new_global_ref(&udf)?;
        let bridge_class_local = env.find_class("org/apache/datafusion/internal/JniBridge")?;
        let bridge_class = env.new_global_ref(&bridge_class_local)?;
        let invoke_method = env.get_static_method_id(
            &bridge_class_local,
            "invokeScalarUdf",
            "(Lorg/apache/datafusion/ScalarFunction;JJJJ[BJJI)B",
        )?;

        let java_udf = crate::udf::JavaScalarUdf {
            name: name.clone(),
            signature,
            return_field,
            udf_global_ref,
            bridge_class,
            invoke_method,
        };
        ctx.register_udf(ScalarUDF::new_from_impl(java_udf));
        Ok(())
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_registerTableNative<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    name: JString<'local>,
    schema_ipc_bytes: JByteArray<'local>,
    provider: JObject<'local>,
) {
    try_unwrap_or_throw(&mut env, (), |env| -> JniResult<()> {
        if handle == 0 {
            return Err("SessionContext handle is null".into());
        }
        // SAFETY: handle is a valid Box<SessionContext> allocated by createSessionContext.
        let ctx = unsafe { &*(handle as *const SessionContext) };
        let name: String = env.get_string(&name)?.into();

        let schema = crate::schema::decode_optional_schema(env, schema_ipc_bytes)?
            .ok_or("schema bytes were null")?;
        let schema = Arc::new(schema);

        let source_global_ref = Arc::new(env.new_global_ref(&provider)?);
        let bridge_class_local = env.find_class("org/apache/datafusion/internal/JniBridge")?;
        let bridge_class = Arc::new(env.new_global_ref(&bridge_class_local)?);
        let invoke_method = env.get_static_method_id(
            &bridge_class_local,
            "invokeTableScan",
            "(Lorg/apache/datafusion/TableProvider;J)V",
        )?;

        let java_tp = crate::table_provider::JavaTableProvider {
            name: name.clone(),
            schema,
            source_global_ref,
            bridge_class,
            invoke_method,
        };
        let _ = ctx.register_table(name.as_str(), Arc::new(java_tp))?;
        Ok(())
    })
}
