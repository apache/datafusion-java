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

use std::str::FromStr;

use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::error::DataFusionError;
use datafusion::prelude::{CsvReadOptions, SessionContext};
use jni::objects::{JByteArray, JClass, JString};
use jni::sys::{jboolean, jbyte, jlong};
use jni::JNIEnv;

use crate::errors::{try_unwrap_or_throw, JniResult};
use crate::runtime;

#[allow(clippy::too_many_arguments)]
fn with_csv_options<R>(
    env: &mut JNIEnv,
    has_header: jboolean,
    delimiter: jbyte,
    quote: jbyte,
    terminator_set: jboolean,
    terminator_value: jbyte,
    escape_set: jboolean,
    escape_value: jbyte,
    comment_set: jboolean,
    comment_value: jbyte,
    newlines_in_values_set: jboolean,
    newlines_in_values_value: jboolean,
    schema_infer_max_records: jlong,
    file_extension: JString,
    file_compression_type: JString,
    schema_ipc_bytes: JByteArray,
    f: impl FnOnce(CsvReadOptions) -> JniResult<R>,
) -> JniResult<R> {
    let file_ext: String = env.get_string(&file_extension)?.into();
    let compression: String = env.get_string(&file_compression_type)?.into();
    let compression = FileCompressionType::from_str(&compression)?;

    let schema: Option<Schema> = if !schema_ipc_bytes.is_null() {
        let bytes: Vec<u8> = env.convert_byte_array(&schema_ipc_bytes)?;
        let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
        Some((*reader.schema()).clone())
    } else {
        None
    };

    let mut opts = CsvReadOptions::new()
        .has_header(has_header != 0)
        .delimiter(delimiter as u8)
        .quote(quote as u8)
        .file_extension(&file_ext)
        .file_compression_type(compression);

    if terminator_set != 0 {
        opts = opts.terminator(Some(terminator_value as u8));
    }
    if escape_set != 0 {
        opts = opts.escape(escape_value as u8);
    }
    if comment_set != 0 {
        opts = opts.comment(comment_value as u8);
    }
    if newlines_in_values_set != 0 {
        opts = opts.newlines_in_values(newlines_in_values_value != 0);
    }
    if schema_infer_max_records >= 0 {
        opts = opts.schema_infer_max_records(schema_infer_max_records as usize);
    }
    if let Some(ref s) = schema {
        opts = opts.schema(s);
    }

    f(opts)
}

#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_registerCsvWithOptions<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    name: JString<'local>,
    path: JString<'local>,
    has_header: jboolean,
    delimiter: jbyte,
    quote: jbyte,
    terminator_set: jboolean,
    terminator_value: jbyte,
    escape_set: jboolean,
    escape_value: jbyte,
    comment_set: jboolean,
    comment_value: jbyte,
    newlines_in_values_set: jboolean,
    newlines_in_values_value: jboolean,
    schema_infer_max_records: jlong,
    file_extension: JString<'local>,
    file_compression_type: JString<'local>,
    schema_ipc_bytes: JByteArray<'local>,
) {
    try_unwrap_or_throw(&mut env, (), |env| -> JniResult<()> {
        if handle == 0 {
            return Err("SessionContext handle is null".into());
        }
        let ctx = unsafe { &*(handle as *const SessionContext) };
        let name: String = env.get_string(&name)?.into();
        let path: String = env.get_string(&path)?.into();
        with_csv_options(
            env,
            has_header,
            delimiter,
            quote,
            terminator_set,
            terminator_value,
            escape_set,
            escape_value,
            comment_set,
            comment_value,
            newlines_in_values_set,
            newlines_in_values_value,
            schema_infer_max_records,
            file_extension,
            file_compression_type,
            schema_ipc_bytes,
            |opts| {
                runtime().block_on(async {
                    ctx.register_csv(&name, &path, opts).await?;
                    Ok::<(), DataFusionError>(())
                })?;
                Ok(())
            },
        )
    })
}

#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_readCsvWithOptions<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    path: JString<'local>,
    has_header: jboolean,
    delimiter: jbyte,
    quote: jbyte,
    terminator_set: jboolean,
    terminator_value: jbyte,
    escape_set: jboolean,
    escape_value: jbyte,
    comment_set: jboolean,
    comment_value: jbyte,
    newlines_in_values_set: jboolean,
    newlines_in_values_value: jboolean,
    schema_infer_max_records: jlong,
    file_extension: JString<'local>,
    file_compression_type: JString<'local>,
    schema_ipc_bytes: JByteArray<'local>,
) -> jlong {
    try_unwrap_or_throw(&mut env, 0, |env| -> JniResult<jlong> {
        if handle == 0 {
            return Err("SessionContext handle is null".into());
        }
        let ctx = unsafe { &*(handle as *const SessionContext) };
        let path: String = env.get_string(&path)?.into();
        with_csv_options(
            env,
            has_header,
            delimiter,
            quote,
            terminator_set,
            terminator_value,
            escape_set,
            escape_value,
            comment_set,
            comment_value,
            newlines_in_values_set,
            newlines_in_values_value,
            schema_infer_max_records,
            file_extension,
            file_compression_type,
            schema_ipc_bytes,
            |opts| {
                let df = runtime().block_on(ctx.read_csv(path, opts))?;
                Ok(Box::into_raw(Box::new(df)) as jlong)
            },
        )
    })
}
