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

//! Per-partition execution of a planned DataFrame.
//!
//! `Java_org_apache_datafusion_DataFrame_createPartitionedExecution` plans a
//! DataFrame exactly once and returns a handle over the resulting physical
//! plan. The handle supports concurrent `executeStreamPartition` calls from
//! multiple JVM threads -- `ExecutionPlan` and `TaskContext` are `Send + Sync`
//! and every call only clones their `Arc`s before producing an independent
//! `SendableRecordBatchStream`. Re-executing the same partition index twice
//! (Spark task retry / speculative execution) opens its own stream, but only
//! succeeds when every operator in that partition's pipeline supports repeated
//! `execute()` -- stateless scans (MemTable, table providers) do, while
//! `RepartitionExec` pipelines panic on the second call because their
//! per-partition channel receivers are single-use.
//!
//! The single unsafe interleaving is `closePartitionedExecution` racing an
//! in-flight call on the same handle. The Java consumer (the Spark connector's
//! shared-scan cache) prevents it with a refcount that covers every open
//! reader; `PartitionedExecution`'s Javadoc states the contract for any other
//! caller.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ffi_stream::FFI_ArrowArrayStream;
use datafusion::dataframe::DataFrame;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use jni::objects::JClass;
use jni::sys::{jint, jlong};
use jni::JNIEnv;

use crate::errors::{try_unwrap_or_throw, JniResult};
use crate::{runtime, StreamingReader};

pub(crate) struct PartitionedExecutionState {
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_DataFrame_createPartitionedExecution<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    try_unwrap_or_throw(&mut env, 0, |_env| -> JniResult<jlong> {
        if handle == 0 {
            return Err("DataFrame handle is null".into());
        }
        // Consuming, like executeStreamDataFrame: the Java side zeroes its
        // handle before calling, so this Box is the last owner.
        let df = unsafe { *Box::from_raw(handle as *mut DataFrame) };

        // task_ctx() borrows; capture it before create_physical_plan consumes
        // the DataFrame.
        let task_ctx = Arc::new(df.task_ctx());
        let plan = runtime().block_on(df.create_physical_plan())?;

        let state = PartitionedExecutionState { plan, task_ctx };
        Ok(Box::into_raw(Box::new(state)) as jlong)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_PartitionedExecution_partitionCountNative<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jint {
    try_unwrap_or_throw(&mut env, 0, |_env| -> JniResult<jint> {
        if handle == 0 {
            return Err("PartitionedExecution handle is null".into());
        }
        let state = unsafe { &*(handle as *const PartitionedExecutionState) };
        Ok(state
            .plan
            .properties()
            .output_partitioning()
            .partition_count() as jint)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_PartitionedExecution_executeStreamPartition<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    partition: jint,
    ffi_stream_addr: jlong,
) {
    try_unwrap_or_throw(&mut env, (), |_env| -> JniResult<()> {
        if handle == 0 {
            return Err("PartitionedExecution handle is null".into());
        }
        if ffi_stream_addr == 0 {
            return Err("ffi stream address is null".into());
        }
        let state = unsafe { &*(handle as *const PartitionedExecutionState) };

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
            let _guard = runtime().enter();
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

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_PartitionedExecution_closePartitionedExecution<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    try_unwrap_or_throw(&mut env, (), |_env| -> JniResult<()> {
        if handle == 0 {
            return Err("PartitionedExecution handle is null".into());
        }
        drop(unsafe { Box::from_raw(handle as *mut PartitionedExecutionState) });
        Ok(())
    })
}
