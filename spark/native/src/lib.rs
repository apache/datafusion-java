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

//! Generic FFI-path cdylib behind `io.datafusion.spark.FfiHelperNative`.
//!
//! Thin JNI shims: each entry point imports the bridge cdylib's raw
//! `FFI_TableProvider` pointer and delegates to the scan machinery in
//! `datafusion-spark-bridge` (widening, session from pinned config,
//! projection, proto filters, planning, partition streams). Bridges that
//! statically link their provider use `datafusion_spark_bridge::export_bridge!`
//! with their own JNI class name instead of this library.

use datafusion_spark_bridge::ffi::import_ffi_provider;
use datafusion_spark_bridge::scan;
use jni::objects::{JClass, JObjectArray};
use jni::sys::{jbyteArray, jint, jlong};
use jni::JNIEnv;

#[no_mangle]
pub extern "system" fn Java_io_datafusion_spark_FfiHelperNative_providerSchemaIpc<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    ffi_raw_ptr: jlong,
) -> jbyteArray {
    scan::provider_schema_ipc(&mut env, |_env| import_ffi_provider(ffi_raw_ptr))
}

#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_io_datafusion_spark_FfiHelperNative_createScan<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    ffi_raw_ptr: jlong,
    target_partitions: jint,
    batch_size: jint,
    option_keys: JObjectArray<'local>,
    option_values: JObjectArray<'local>,
    projection_columns: JObjectArray<'local>,
    filter_protos: JObjectArray<'local>,
) -> jlong {
    scan::create_scan(
        &mut env,
        |_env| import_ffi_provider(ffi_raw_ptr),
        target_partitions,
        batch_size,
        &option_keys,
        &option_values,
        &projection_columns,
        &filter_protos,
    )
}

#[no_mangle]
pub extern "system" fn Java_io_datafusion_spark_FfiHelperNative_partitionCount<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jint {
    scan::partition_count(&mut env, handle)
}

#[no_mangle]
pub extern "system" fn Java_io_datafusion_spark_FfiHelperNative_executeStreamPartition<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    partition: jint,
    ffi_stream_addr: jlong,
) {
    scan::execute_stream_partition(&mut env, handle, partition, ffi_stream_addr)
}

#[no_mangle]
pub extern "system" fn Java_io_datafusion_spark_FfiHelperNative_executeStream<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    ffi_stream_addr: jlong,
) {
    scan::execute_stream(&mut env, handle, ffi_stream_addr)
}

#[no_mangle]
pub extern "system" fn Java_io_datafusion_spark_FfiHelperNative_closeScan<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    scan::close_scan(&mut env, handle)
}
