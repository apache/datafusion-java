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

//! Planning and execution of a Spark scan, provider-source-agnostic.
//!
//! Every function here is the body of one JNI entry point; the caller (the
//! generic FFI cdylib, or a static bridge's `export_bridge!` expansion)
//! supplies only how the provider is obtained, as a `make` closure. The
//! provider is wrapped in a [`WideningTableProvider`] here, so both binding
//! styles get identical Spark-compatible Arrow types.
//!
//! [`create_scan`] registers the widened provider on a private
//! `SessionContext` built from the caller-pinned config, applies the pruned
//! projection and the proto-encoded pushed filters, and plans exactly once.
//! The returned handle supports:
//!
//!   - [`partition_count`] — output partitions of the physical plan
//!     (shared-scan mode probes this on the driver and indexes tasks by it);
//!   - [`execute_stream_partition`] — an independent stream over ONE plan
//!     partition, concurrently callable from multiple JVM threads
//!     (`ExecutionPlan` and `TaskContext` are `Send + Sync`; each call only
//!     clones their `Arc`s). Re-executing the same partition index (Spark
//!     task retry / speculative execution) opens its own stream, but only
//!     succeeds when every operator in that partition's pipeline supports
//!     repeated `execute()` — stateless scans do, `RepartitionExec`
//!     pipelines do not;
//!   - [`execute_stream`] — the whole plan as one stream (legacy
//!     per-partition payload mode, where the provider itself is the task's
//!     slice);
//!   - [`close_scan`] — drop the plan. The single unsafe interleaving is
//!     closing a handle that still has an in-flight call; the Java consumer
//!     (the shared-scan cache) prevents it with a refcount covering every
//!     open reader.
//!
//! Pinned-config determinism: the driver resolves `target_partitions` /
//! `batch_size` / option overrides once and ships them to every executor, so
//! a plan that yields N partitions on the driver yields N everywhere. This
//! module applies whatever it is handed and stays policy-free.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ffi_stream::FFI_ArrowArrayStream;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::catalog::TableProvider;
use datafusion::dataframe::DataFrame;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{execute_stream as df_execute_stream, ExecutionPlan};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_jni_common::errors::{try_unwrap_or_throw, JniResult};
use datafusion_jni_common::StreamingReader;
use datafusion_proto::logical_plan::from_proto::parse_expr;
use datafusion_proto::logical_plan::DefaultLogicalExtensionCodec;
use datafusion_proto::protobuf::LogicalExprNode;
use jni::objects::{JByteArray, JObjectArray, JString};
use jni::sys::{jbyteArray, jint, jlong};
use jni::JNIEnv;
use prost::Message;

use crate::runtime_handle;
use crate::widening::WideningTableProvider;

/// Registration name of the (single) provider on the scan's private context.
/// Never surfaces in SQL — the plan is built through the DataFrame API — so
/// no quoting/collision concerns.
const SCAN_TABLE_NAME: &str = "df_spark_scan";

struct ScanState {
    /// Kept alive for the plan's lifetime; the registered provider and the
    /// runtime env both hang off it.
    _ctx: SessionContext,
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
}

fn widen(provider: Arc<dyn TableProvider>) -> Arc<dyn TableProvider> {
    Arc::new(WideningTableProvider::new(provider))
}

fn collect_string_array(env: &mut JNIEnv, arr: &JObjectArray) -> JniResult<Vec<String>> {
    if arr.is_null() {
        return Ok(Vec::new());
    }
    let len = env.get_array_length(arr)?;
    let mut owned: Vec<String> = Vec::with_capacity(len as usize);
    for i in 0..len {
        let elem = env.get_object_array_element(arr, i)?;
        let jstr: JString = elem.into();
        owned.push(env.get_string(&jstr)?.into());
    }
    Ok(owned)
}

fn collect_byte_arrays(env: &mut JNIEnv, arr: &JObjectArray) -> JniResult<Vec<Vec<u8>>> {
    if arr.is_null() {
        return Ok(Vec::new());
    }
    let len = env.get_array_length(arr)?;
    let mut owned: Vec<Vec<u8>> = Vec::with_capacity(len as usize);
    for i in 0..len {
        let elem = env.get_object_array_element(arr, i)?;
        let bytes: JByteArray = elem.into();
        owned.push(env.convert_byte_array(&bytes)?);
    }
    Ok(owned)
}

/// Driver-side schema probe: widened Arrow schema of the provider, as IPC
/// bytes (deserialized JVM-side with `MessageSerializer.deserializeSchema`).
/// `make` runs once; the provider drops before returning.
pub fn provider_schema_ipc(
    env: &mut JNIEnv,
    make: impl FnOnce(&mut JNIEnv) -> JniResult<Arc<dyn TableProvider>>,
) -> jbyteArray {
    try_unwrap_or_throw(env, std::ptr::null_mut(), |env| -> JniResult<jbyteArray> {
        let widened = widen(make(env)?);
        let schema = widened.schema();
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, schema.as_ref())?;
            writer.finish()?;
        }
        let arr = env.byte_array_from_slice(&buf)?;
        Ok(arr.into_raw())
    })
}

/// Build the scan: widen the provider from `make`, register it on a private
/// context with the pinned config, apply projection + pushed filters, plan
/// once.
///
/// `target_partitions` / `batch_size` <= 0 leave the DataFusion defaults;
/// `option_keys`/`option_values` are parallel arrays of config overrides;
/// empty `projection_columns` selects all columns; each element of
/// `filter_protos` is a serialized `datafusion.LogicalExprNode`.
#[allow(clippy::too_many_arguments)]
pub fn create_scan(
    env: &mut JNIEnv,
    make: impl FnOnce(&mut JNIEnv) -> JniResult<Arc<dyn TableProvider>>,
    target_partitions: jint,
    batch_size: jint,
    option_keys: &JObjectArray,
    option_values: &JObjectArray,
    projection_columns: &JObjectArray,
    filter_protos: &JObjectArray,
) -> jlong {
    try_unwrap_or_throw(env, 0, |env| -> JniResult<jlong> {
        let widened = widen(make(env)?);

        let keys = collect_string_array(env, option_keys)?;
        let values = collect_string_array(env, option_values)?;
        if keys.len() != values.len() {
            return Err(format!(
                "option key/value arrays differ in length: {} vs {}",
                keys.len(),
                values.len()
            )
            .into());
        }
        let projection = collect_string_array(env, projection_columns)?;
        let filters = collect_byte_arrays(env, filter_protos)?;

        let mut config = SessionConfig::new();
        if target_partitions > 0 {
            config = config.with_target_partitions(target_partitions as usize);
        }
        if batch_size > 0 {
            config = config.with_batch_size(batch_size as usize);
        }
        for (key, value) in keys.iter().zip(values.iter()) {
            config.options_mut().set(key, value)?;
        }

        let ctx = SessionContext::new_with_config(config);
        ctx.register_table(SCAN_TABLE_NAME, widened)?;

        let mut df: DataFrame = runtime_handle().block_on(ctx.table(SCAN_TABLE_NAME))?;
        if !projection.is_empty() {
            let refs: Vec<&str> = projection.iter().map(String::as_str).collect();
            df = df.select_columns(&refs)?;
        }
        for bytes in &filters {
            let node = LogicalExprNode::decode(bytes.as_slice())?;
            // TaskContext implements FunctionRegistry; the default codec is
            // enough because the translator only emits column/literal/builtin
            // expressions.
            let registry = df.task_ctx();
            let expr = parse_expr(&node, &registry, &DefaultLogicalExtensionCodec {})?;
            df = df.filter(expr)?;
        }

        // task_ctx() borrows; capture before create_physical_plan consumes df.
        let task_ctx = Arc::new(df.task_ctx());
        let plan = runtime_handle().block_on(df.create_physical_plan())?;

        let state = ScanState {
            _ctx: ctx,
            plan,
            task_ctx,
        };
        Ok(Box::into_raw(Box::new(state)) as jlong)
    })
}

/// Output partition count of the planned physical plan.
pub fn partition_count(env: &mut JNIEnv, handle: jlong) -> jint {
    try_unwrap_or_throw(env, 0, |_env| -> JniResult<jint> {
        if handle == 0 {
            return Err("scan handle is null".into());
        }
        let state = unsafe { &*(handle as *const ScanState) };
        Ok(state
            .plan
            .properties()
            .output_partitioning()
            .partition_count() as jint)
    })
}

/// Open an independent stream over one plan partition, writing an
/// `FFI_ArrowArrayStream` into the caller-allocated struct at
/// `ffi_stream_addr`.
pub fn execute_stream_partition(
    env: &mut JNIEnv,
    handle: jlong,
    partition: jint,
    ffi_stream_addr: jlong,
) {
    try_unwrap_or_throw(env, (), |_env| -> JniResult<()> {
        if handle == 0 {
            return Err("scan handle is null".into());
        }
        if ffi_stream_addr == 0 {
            return Err("ffi stream address is null".into());
        }
        let state = unsafe { &*(handle as *const ScanState) };

        let partition_count = state
            .plan
            .properties()
            .output_partitioning()
            .partition_count();
        if partition < 0 || partition as usize >= partition_count {
            return Err(format!(
                "partition index {partition} out of range: plan has {partition_count} partition(s)"
            )
            .into());
        }

        let plan = Arc::clone(&state.plan);
        let task_ctx = Arc::clone(&state.task_ctx);
        let schema: SchemaRef = plan.schema();

        // ExecutionPlan::execute is synchronous, but operators may
        // tokio::spawn at execute() time (RepartitionExec et al.), which
        // requires a runtime context to be entered.
        let stream = {
            let _guard = runtime_handle().enter();
            plan.execute(partition as usize, task_ctx)?
        };

        let reader = StreamingReader { schema, stream };
        let ffi = FFI_ArrowArrayStream::new(Box::new(reader));
        unsafe {
            std::ptr::write(ffi_stream_addr as *mut FFI_ArrowArrayStream, ffi);
        }
        Ok(())
    })
}

/// Whole-plan stream for legacy per-partition payload mode (the provider
/// itself is the task's slice, so all plan partitions merge into one reader).
pub fn execute_stream(env: &mut JNIEnv, handle: jlong, ffi_stream_addr: jlong) {
    try_unwrap_or_throw(env, (), |_env| -> JniResult<()> {
        if handle == 0 {
            return Err("scan handle is null".into());
        }
        if ffi_stream_addr == 0 {
            return Err("ffi stream address is null".into());
        }
        let state = unsafe { &*(handle as *const ScanState) };

        let plan = Arc::clone(&state.plan);
        let task_ctx = Arc::clone(&state.task_ctx);
        let schema: SchemaRef = plan.schema();

        // execute_stream coalesces multi-partition plans behind one stream.
        let stream = {
            let _guard = runtime_handle().enter();
            df_execute_stream(plan, task_ctx)?
        };

        let reader = StreamingReader { schema, stream };
        let ffi = FFI_ArrowArrayStream::new(Box::new(reader));
        unsafe {
            std::ptr::write(ffi_stream_addr as *mut FFI_ArrowArrayStream, ffi);
        }
        Ok(())
    })
}

/// Drop the planned scan. Must not race an in-flight stream-open on the same
/// handle; the Java consumer's refcount enforces this.
pub fn close_scan(env: &mut JNIEnv, handle: jlong) {
    try_unwrap_or_throw(env, (), |_env| -> JniResult<()> {
        if handle == 0 {
            return Err("scan handle is null".into());
        }
        drop(unsafe { Box::from_raw(handle as *mut ScanState) });
        Ok(())
    })
}
