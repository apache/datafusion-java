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

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::dataframe::DataFrame;
use datafusion::prelude::SessionContext;
use datafusion_proto::logical_plan::{AsLogicalPlan, DefaultLogicalExtensionCodec};
use datafusion_proto::protobuf::LogicalPlanNode;
use jni::objects::{JByteArray, JClass, JString};
use jni::sys::{jbyteArray, jlong};
use jni::JNIEnv;
use prost::Message;

use crate::errors::{try_unwrap_or_throw, JniResult};
use crate::runtime;

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_createDataFrameFromProto<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    plan_bytes: JByteArray<'local>,
) -> jlong {
    try_unwrap_or_throw(&mut env, 0, |env| -> JniResult<jlong> {
        if handle == 0 {
            return Err("SessionContext handle is null".into());
        }
        let ctx = unsafe { &*(handle as *const SessionContext) };
        let bytes: Vec<u8> = env.convert_byte_array(&plan_bytes)?;
        let node = LogicalPlanNode::decode(bytes.as_slice())?;
        let codec = DefaultLogicalExtensionCodec {};
        let task_ctx = ctx.task_ctx();
        let plan = node.try_into_logical_plan(task_ctx.as_ref(), &codec)?;
        let df: DataFrame = runtime().block_on(ctx.execute_logical_plan(plan))?;
        Ok(Box::into_raw(Box::new(df)) as jlong)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_tableSchemaIpc<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    name: JString<'local>,
) -> jbyteArray {
    try_unwrap_or_throw(
        &mut env,
        std::ptr::null_mut(),
        |env| -> JniResult<jbyteArray> {
            if handle == 0 {
                return Err("SessionContext handle is null".into());
            }
            let ctx = unsafe { &*(handle as *const SessionContext) };
            let name: String = env.get_string(&name)?.into();

            let df = runtime().block_on(ctx.table(name.as_str()))?;
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
