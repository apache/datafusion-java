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

use datafusion::error::DataFusionError;
use datafusion::execution::options::ArrowReadOptions;
use datafusion::prelude::SessionContext;
use jni::objects::{JByteArray, JClass, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use prost::Message;

use crate::errors::{try_unwrap_or_throw, JniResult};
use crate::proto_gen::ArrowReadOptionsProto;
use crate::runtime;
use crate::schema::decode_optional_schema;

fn with_arrow_options<R>(
    env: &mut JNIEnv,
    options_bytes: JByteArray,
    schema_ipc_bytes: JByteArray,
    f: impl FnOnce(ArrowReadOptions) -> JniResult<R>,
) -> JniResult<R> {
    let bytes: Vec<u8> = env.convert_byte_array(&options_bytes)?;
    let p = ArrowReadOptionsProto::decode(bytes.as_slice())?;

    let schema = decode_optional_schema(env, schema_ipc_bytes)?;

    // ArrowReadOptions exposes `file_extension` as a public field (not a builder
    // setter); `schema` is the only field with a fluent setter. Build via
    // struct-update syntax to avoid clippy::field_reassign_with_default.
    let file_ext = p.file_extension;
    let mut opts = ArrowReadOptions {
        file_extension: &file_ext,
        ..ArrowReadOptions::default()
    };
    if let Some(ref s) = schema {
        opts = opts.schema(s);
    }

    f(opts)
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_registerArrowWithOptions<
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
        with_arrow_options(env, options_bytes, schema_ipc_bytes, |opts| {
            runtime().block_on(async {
                ctx.register_arrow(&name, &path, opts).await?;
                Ok::<(), DataFusionError>(())
            })?;
            Ok(())
        })
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_readArrowWithOptions<'local>(
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
        with_arrow_options(env, options_bytes, schema_ipc_bytes, |opts| {
            let df = runtime().block_on(ctx.read_arrow(path, opts))?;
            Ok(Box::into_raw(Box::new(df)) as jlong)
        })
    })
}
