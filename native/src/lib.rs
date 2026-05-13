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

pub(crate) mod session_options {
    include!(concat!(env!("OUT_DIR"), "/datafusion_java.rs"));
}

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::ffi_stream::FFI_ArrowArrayStream;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::record_batch::RecordBatchIterator;
use datafusion::dataframe::DataFrame;
use datafusion::error::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use jni::objects::{JByteArray, JClass, JObjectArray, JString};
use jni::sys::{jboolean, jint, jlong};
use jni::JNIEnv;
use prost::Message;
use tokio::runtime::Runtime;

use crate::errors::{try_unwrap_or_throw, JniResult};
use crate::session_options::SessionOptions;

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

        let mut runtime = RuntimeEnvBuilder::new();
        if let Some(mem) = opts.memory_limit {
            runtime = runtime.with_memory_limit(mem.max_memory_bytes as usize, mem.memory_fraction);
        }
        if let Some(dir) = opts.temp_directory {
            runtime = runtime.with_temp_file_path(PathBuf::from(dir));
        }

        let runtime_env = runtime.build()?;
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

#[allow(clippy::too_many_arguments)]
fn with_parquet_options<R>(
    env: &mut JNIEnv,
    file_extension: JString,
    parquet_pruning_set: jboolean,
    parquet_pruning_value: jboolean,
    skip_metadata_set: jboolean,
    skip_metadata_value: jboolean,
    metadata_size_hint: jlong,
    schema_ipc_bytes: JByteArray,
    f: impl FnOnce(ParquetReadOptions) -> JniResult<R>,
) -> JniResult<R> {
    let file_ext: String = env.get_string(&file_extension)?.into();

    let schema: Option<Schema> = if !schema_ipc_bytes.is_null() {
        let bytes: Vec<u8> = env.convert_byte_array(&schema_ipc_bytes)?;
        let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
        Some((*reader.schema()).clone())
    } else {
        None
    };

    let mut opts = ParquetReadOptions::default().file_extension(&file_ext);
    if parquet_pruning_set != 0 {
        opts = opts.parquet_pruning(parquet_pruning_value != 0);
    }
    if skip_metadata_set != 0 {
        opts = opts.skip_metadata(skip_metadata_value != 0);
    }
    if metadata_size_hint >= 0 {
        opts = opts.metadata_size_hint(Some(metadata_size_hint as usize));
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
    file_extension: JString<'local>,
    parquet_pruning_set: jboolean,
    parquet_pruning_value: jboolean,
    skip_metadata_set: jboolean,
    skip_metadata_value: jboolean,
    metadata_size_hint: jlong,
    schema_ipc_bytes: JByteArray<'local>,
) {
    try_unwrap_or_throw(&mut env, (), |env| -> JniResult<()> {
        if handle == 0 {
            return Err("SessionContext handle is null".into());
        }
        let ctx = unsafe { &*(handle as *const SessionContext) };
        let name: String = env.get_string(&name)?.into();
        let path: String = env.get_string(&path)?.into();
        with_parquet_options(
            env,
            file_extension,
            parquet_pruning_set,
            parquet_pruning_value,
            skip_metadata_set,
            skip_metadata_value,
            metadata_size_hint,
            schema_ipc_bytes,
            |opts| {
                runtime().block_on(async {
                    ctx.register_parquet(&name, &path, opts).await?;
                    Ok::<(), DataFusionError>(())
                })?;
                Ok(())
            },
        )
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_readParquetWithOptions<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    path: JString<'local>,
    file_extension: JString<'local>,
    parquet_pruning_set: jboolean,
    parquet_pruning_value: jboolean,
    skip_metadata_set: jboolean,
    skip_metadata_value: jboolean,
    metadata_size_hint: jlong,
    schema_ipc_bytes: JByteArray<'local>,
) -> jlong {
    try_unwrap_or_throw(&mut env, 0, |env| -> JniResult<jlong> {
        if handle == 0 {
            return Err("SessionContext handle is null".into());
        }
        let ctx = unsafe { &*(handle as *const SessionContext) };
        let path: String = env.get_string(&path)?.into();
        with_parquet_options(
            env,
            file_extension,
            parquet_pruning_set,
            parquet_pruning_value,
            skip_metadata_set,
            skip_metadata_value,
            metadata_size_hint,
            schema_ipc_bytes,
            |opts| {
                let df = runtime().block_on(ctx.read_parquet(path, opts))?;
                Ok(Box::into_raw(Box::new(df)) as jlong)
            },
        )
    })
}
