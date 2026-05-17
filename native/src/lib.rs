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

mod csv;
mod errors;
mod proto;
mod schema;
mod udf;

pub(crate) mod proto_gen {
    include!(concat!(env!("OUT_DIR"), "/datafusion_java.rs"));
}

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ffi_stream::FFI_ArrowArrayStream;
use datafusion::arrow::record_batch::RecordBatchIterator;
use datafusion::config::TableParquetOptions;
use datafusion::dataframe::DataFrame;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::error::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::{ScalarUDF, Signature};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use jni::objects::{JByteArray, JClass, JObject, JObjectArray, JString};
use jni::sys::{jboolean, jint, jlong};
use jni::JNIEnv;
use jni::JavaVM;
use prost::Message;
use tokio::runtime::Runtime;

use crate::errors::{try_unwrap_or_throw, JniResult};
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
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| Runtime::new().expect("failed to create Tokio runtime"))
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_createSessionContext<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jlong {
    try_unwrap_or_throw(&mut env, 0, |_env| -> JniResult<jlong> {
        let ctx = SessionContext::new();
        Ok(Box::into_raw(Box::new(ctx)) as jlong)
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

        let runtime_env = runtime_builder.build()?;
        let ctx = SessionContext::new_with_config_rt(config, Arc::new(runtime_env));

        Ok(Box::into_raw(Box::new(ctx)) as jlong)
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

        let mut write_opts = DataFrameWriteOptions::new();
        if single_file_output_set != 0 {
            write_opts = write_opts.with_single_file_output(single_file_output_value != 0);
        }

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
            unsafe {
                drop(Box::from_raw(handle as *mut SessionContext));
            }
        }
        Ok(())
    })
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
        let return_type = fields[0].data_type().clone();
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
            "(Lorg/apache/datafusion/ScalarFunction;JJJJI)V",
        )?;

        let java_udf = crate::udf::JavaScalarUdf {
            name: name.clone(),
            signature,
            return_type,
            udf_global_ref,
            bridge_class,
            invoke_method,
        };
        ctx.register_udf(ScalarUDF::new_from_impl(java_udf));
        Ok(())
    })
}
