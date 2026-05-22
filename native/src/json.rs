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

use datafusion::common::config::JsonOptions;
use datafusion::common::parsers::CompressionTypeVariant;
use datafusion::dataframe::{DataFrame, DataFrameWriteOptions};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::error::DataFusionError;
use datafusion::prelude::JsonReadOptions;
use datafusion::prelude::SessionContext;
use jni::objects::{JByteArray, JClass, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use prost::Message;

use crate::errors::{try_unwrap_or_throw, JniResult};
use crate::proto_gen::{
    FileCompressionType as ProtoFileCompressionType, JsonWriteOptionsProto, NdJsonReadOptionsProto,
};
use crate::runtime;
use crate::schema::decode_optional_schema;

fn with_json_options<R>(
    env: &mut JNIEnv,
    options_bytes: JByteArray,
    schema_ipc_bytes: JByteArray,
    f: impl FnOnce(JsonReadOptions) -> JniResult<R>,
) -> JniResult<R> {
    let bytes: Vec<u8> = env.convert_byte_array(&options_bytes)?;
    let p = NdJsonReadOptionsProto::decode(bytes.as_slice())?;

    let schema = decode_optional_schema(env, schema_ipc_bytes)?;

    let compression = match p.file_compression_type() {
        ProtoFileCompressionType::Unspecified => {
            return Err("NdJsonReadOptionsProto.file_compression_type is UNSPECIFIED".into());
        }
        ProtoFileCompressionType::Uncompressed => FileCompressionType::UNCOMPRESSED,
        ProtoFileCompressionType::Gzip => FileCompressionType::GZIP,
        ProtoFileCompressionType::Bzip2 => FileCompressionType::BZIP2,
        ProtoFileCompressionType::Xz => FileCompressionType::XZ,
        ProtoFileCompressionType::Zstd => FileCompressionType::ZSTD,
    };

    let file_ext = p.file_extension;
    let mut opts = JsonReadOptions::default()
        .file_extension(&file_ext)
        .file_compression_type(compression);

    if let Some(n) = p.schema_infer_max_records {
        opts = opts.schema_infer_max_records(n as usize);
    }
    if let Some(ref s) = schema {
        opts = opts.schema(s);
    }

    f(opts)
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_registerJsonWithOptions<'local>(
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
        with_json_options(env, options_bytes, schema_ipc_bytes, |opts| {
            runtime().block_on(async {
                ctx.register_json(&name, &path, opts).await?;
                Ok::<(), DataFusionError>(())
            })?;
            Ok(())
        })
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_readJsonWithOptions<'local>(
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
        with_json_options(env, options_bytes, schema_ipc_bytes, |opts| {
            let df = runtime().block_on(ctx.read_json(path, opts))?;
            Ok(Box::into_raw(Box::new(df)) as jlong)
        })
    })
}

fn proto_compression_to_variant(p: ProtoFileCompressionType) -> JniResult<CompressionTypeVariant> {
    match p {
        ProtoFileCompressionType::Unspecified => {
            Err("JsonWriteOptionsProto.file_compression_type is UNSPECIFIED".into())
        }
        ProtoFileCompressionType::Uncompressed => Ok(CompressionTypeVariant::UNCOMPRESSED),
        ProtoFileCompressionType::Gzip => Ok(CompressionTypeVariant::GZIP),
        ProtoFileCompressionType::Bzip2 => Ok(CompressionTypeVariant::BZIP2),
        ProtoFileCompressionType::Xz => Ok(CompressionTypeVariant::XZ),
        ProtoFileCompressionType::Zstd => Ok(CompressionTypeVariant::ZSTD),
    }
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_DataFrame_writeJsonWithOptions<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    path: JString<'local>,
    options_bytes: JByteArray<'local>,
) {
    try_unwrap_or_throw(&mut env, (), |env| -> JniResult<()> {
        if handle == 0 {
            return Err("DataFrame handle is null".into());
        }
        let df = unsafe { &*(handle as *const DataFrame) }.clone();
        let path: String = env.get_string(&path)?.into();
        let bytes: Vec<u8> = env.convert_byte_array(&options_bytes)?;
        let p = JsonWriteOptionsProto::decode(bytes.as_slice())?;

        // Decode the file_compression_type field eagerly so an unknown wire
        // value surfaces as a clear error rather than a silent default.
        let compression = if p.file_compression_type.is_some() {
            Some(proto_compression_to_variant(p.file_compression_type())?)
        } else {
            None
        };

        // When the caller left `singleFileOutput` unset, force directory output (`false`)
        // rather than leaving DataFusion in `Automatic` mode. Automatic mode treats paths
        // with an extension (e.g. `out.json`) as single-file targets, which would silently
        // contradict the documented "directory unless overridden" default and surprise any
        // caller that hands writeJson a `.json` path.
        let mut write_opts = DataFrameWriteOptions::new()
            .with_single_file_output(p.single_file_output.unwrap_or(false));
        if !p.partition_cols.is_empty() {
            write_opts = write_opts.with_partition_by(p.partition_cols.clone());
        }

        // Build JsonOptions only when a writer-side knob is set, so the
        // DataFusion default is preserved when the caller passes
        // `new JsonWriteOptions()`. JsonOptions has no fluent setters --
        // the fields are public, so use struct-update syntax (same
        // idiom we use for ArrowReadOptions / AvroReadOptions).
        let writer_opts: Option<JsonOptions> = compression.map(|c| JsonOptions {
            compression: c,
            ..JsonOptions::default()
        });

        runtime().block_on(df.write_json(&path, write_opts, writer_opts))?;
        Ok(())
    })
}
