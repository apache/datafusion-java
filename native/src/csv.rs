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

use datafusion::common::config::CsvOptions;
use datafusion::common::parsers::CompressionTypeVariant;
use datafusion::dataframe::{DataFrame, DataFrameWriteOptions};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::error::DataFusionError;
use datafusion::prelude::{CsvReadOptions, SessionContext};
use jni::objects::{JByteArray, JClass, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use prost::Message;

use crate::errors::{try_unwrap_or_throw, JniResult};
use crate::proto_gen::{
    CsvReadOptionsProto, CsvWriteOptionsProto, FileCompressionType as ProtoFileCompressionType,
};
use crate::runtime;
use crate::schema::decode_optional_schema;

fn with_csv_options<R>(
    env: &mut JNIEnv,
    options_bytes: JByteArray,
    schema_ipc_bytes: JByteArray,
    f: impl FnOnce(CsvReadOptions) -> JniResult<R>,
) -> JniResult<R> {
    let bytes: Vec<u8> = env.convert_byte_array(&options_bytes)?;
    let p = CsvReadOptionsProto::decode(bytes.as_slice())?;

    let schema = decode_optional_schema(env, schema_ipc_bytes)?;

    let compression = match p.file_compression_type() {
        ProtoFileCompressionType::Unspecified => {
            return Err("CsvReadOptionsProto.file_compression_type is UNSPECIFIED".into());
        }
        ProtoFileCompressionType::Uncompressed => FileCompressionType::UNCOMPRESSED,
        ProtoFileCompressionType::Gzip => FileCompressionType::GZIP,
        ProtoFileCompressionType::Bzip2 => FileCompressionType::BZIP2,
        ProtoFileCompressionType::Xz => FileCompressionType::XZ,
        ProtoFileCompressionType::Zstd => FileCompressionType::ZSTD,
    };

    let file_ext = p.file_extension;
    let mut opts = CsvReadOptions::new()
        .has_header(p.has_header)
        .delimiter(p.delimiter as u8)
        .quote(p.quote as u8)
        .file_extension(&file_ext)
        .file_compression_type(compression);

    if let Some(t) = p.terminator {
        opts = opts.terminator(Some(t as u8));
    }
    if let Some(e) = p.escape {
        opts = opts.escape(e as u8);
    }
    if let Some(c) = p.comment {
        opts = opts.comment(c as u8);
    }
    if let Some(n) = p.newlines_in_values {
        opts = opts.newlines_in_values(n);
    }
    if let Some(n) = p.schema_infer_max_records {
        opts = opts.schema_infer_max_records(n as usize);
    }
    if let Some(ref s) = schema {
        opts = opts.schema(s);
    }

    f(opts)
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_registerCsvWithOptions<'local>(
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
        with_csv_options(env, options_bytes, schema_ipc_bytes, |opts| {
            runtime().block_on(async {
                ctx.register_csv(&name, &path, opts).await?;
                Ok::<(), DataFusionError>(())
            })?;
            Ok(())
        })
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_readCsvWithOptions<'local>(
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
        with_csv_options(env, options_bytes, schema_ipc_bytes, |opts| {
            let df = runtime().block_on(ctx.read_csv(path, opts))?;
            Ok(Box::into_raw(Box::new(df)) as jlong)
        })
    })
}

fn proto_compression_to_variant(p: ProtoFileCompressionType) -> JniResult<CompressionTypeVariant> {
    match p {
        ProtoFileCompressionType::Unspecified => {
            Err("CsvWriteOptionsProto.file_compression_type is UNSPECIFIED".into())
        }
        ProtoFileCompressionType::Uncompressed => Ok(CompressionTypeVariant::UNCOMPRESSED),
        ProtoFileCompressionType::Gzip => Ok(CompressionTypeVariant::GZIP),
        ProtoFileCompressionType::Bzip2 => Ok(CompressionTypeVariant::BZIP2),
        ProtoFileCompressionType::Xz => Ok(CompressionTypeVariant::XZ),
        ProtoFileCompressionType::Zstd => Ok(CompressionTypeVariant::ZSTD),
    }
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_DataFrame_writeCsvWithOptions<'local>(
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
        let p = CsvWriteOptionsProto::decode(bytes.as_slice())?;

        // Decode the file_compression_type field eagerly so an unknown wire
        // value surfaces as a clear error rather than a silent default.
        let compression = if p.file_compression_type.is_some() {
            Some(proto_compression_to_variant(p.file_compression_type())?)
        } else {
            None
        };

        let mut write_opts = DataFrameWriteOptions::new();
        if let Some(v) = p.single_file_output {
            write_opts = write_opts.with_single_file_output(v);
        }
        if !p.partition_cols.is_empty() {
            write_opts = write_opts.with_partition_by(p.partition_cols.clone());
        }

        // Build CsvOptions only when at least one writer-side knob is set, so
        // the DataFusion default is preserved when the caller passes
        // `new CsvWriteOptions()`.
        let writer_opts: Option<CsvOptions> = if p.has_header.is_some()
            || p.delimiter.is_some()
            || p.quote.is_some()
            || p.escape.is_some()
            || p.null_value.is_some()
            || compression.is_some()
        {
            let mut o = CsvOptions::default();
            if let Some(v) = p.has_header {
                o = o.with_has_header(v);
            }
            if let Some(v) = p.delimiter {
                o = o.with_delimiter(v as u8);
            }
            if let Some(v) = p.quote {
                o = o.with_quote(v as u8);
            }
            if let Some(v) = p.escape {
                o = o.with_escape(Some(v as u8));
            }
            if let Some(v) = p.null_value {
                o.null_value = Some(v);
            }
            if let Some(v) = compression {
                o = o.with_file_compression_type(v);
            }
            Some(o)
        } else {
            None
        };

        runtime().block_on(df.write_csv(&path, write_opts, writer_opts))?;
        Ok(())
    })
}
