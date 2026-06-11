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

//! SDK for building Spark connector bridges over DataFusion `TableProvider`s.
//!
//! Everything the Spark connector needs DataFusion-side lives here: the
//! Spark-type [`widening`] layer, and the [`scan`] machinery (session from
//! pinned config, projection, proto filters, planning, partition streams).
//! A bridge cdylib depends on this crate and invokes [`export_bridge!`] with
//! a builder that constructs its concrete `TableProvider` from option /
//! partition bytes — one cdylib, no FFI provider boundary; the only foreign
//! interface is JNI plus Arrow's C stream for the results.

pub mod options;
pub mod scan;
pub mod widening;

// Re-exported so `export_bridge!` expansions resolve these crates inside the
// bridge author's crate without extra dependencies, and so builder signatures
// can be written against `datafusion_spark_bridge::datafusion::...`.
pub use datafusion;
pub use datafusion_jni_common::errors::JniResult;
pub use jni;

use tokio::runtime::Handle;

/// Execution environment handed to a bridge's provider builder.
///
/// Provider construction frequently needs async IO (remote catalogs,
/// object-store metadata); run it on the bridge runtime via [`block_on`]
/// rather than creating a runtime of your own.
///
/// [`block_on`]: BridgeContext::block_on
pub struct BridgeContext {
    handle: &'static Handle,
}

impl BridgeContext {
    /// Used by `export_bridge!` expansions; not part of the public API.
    #[doc(hidden)]
    pub fn get() -> Self {
        BridgeContext {
            handle: runtime_handle(),
        }
    }

    /// The cdylib-wide Tokio runtime handle (also the runtime scans run on).
    pub fn handle(&self) -> &Handle {
        self.handle
    }

    /// Block the current (JVM) thread on `fut`, driving it on the bridge
    /// runtime.
    pub fn block_on<F: std::future::Future>(&self, fut: F) -> F::Output {
        self.handle.block_on(fut)
    }
}

/// Per-cdylib Tokio runtime (the singleton from `datafusion-jni-common`).
pub(crate) fn runtime_handle() -> &'static Handle {
    datafusion_jni_common::runtime().handle()
}

/// Generate the JNI entry points for a bridge cdylib.
///
/// `jni_class` is the **underscore-mangled** binary name of the Java class
/// declaring the matching `native` methods: dots become underscores
/// (`com.example.mybridge.BridgeNative` → `"com_example_mybridge_BridgeNative"`).
/// If the class or package name itself contains an underscore, JNI mangling
/// requires it written as `_1`. Per-bridge class names are what let several
/// bridges coexist in one Spark JVM.
///
/// `build_provider` is anything callable as
/// `Fn(&BridgeContext, &[u8], &[u8]) -> JniResult<Arc<dyn TableProvider>>`,
/// receiving the options bytes and partition bytes your JVM factory encoded.
/// The schema probe calls it with empty partition bytes; the scan path passes
/// each task's payload. Return errors boxed from `DataFusionError` to surface
/// as the typed `org.apache.datafusion.*` exception hierarchy.
///
/// The generated Java-side surface (declare these as `static native` on the
/// class named by `jni_class`):
///
/// ```java
/// static native byte[] providerSchemaIpc(byte[] options, byte[] partition);
/// static native long createScan(byte[] options, byte[] partition,
///     int targetPartitions, int batchSize, String[] optionKeys,
///     String[] optionValues, String[] projectionColumns, byte[][] filterProtos);
/// static native int partitionCount(long scanHandle);
/// static native void executeStreamPartition(long scanHandle, int partition, long ffiStreamAddr);
/// static native void executeStream(long scanHandle, long ffiStreamAddr);
/// static native void closeScan(long scanHandle);
/// ```
#[macro_export]
macro_rules! export_bridge {
    (jni_class: $cls:literal, build_provider: $builder:expr $(,)?) => {
        const _: () = {
            use $crate::jni::objects::{JByteArray, JClass, JObjectArray};
            use $crate::jni::sys::{jbyteArray, jint, jlong};
            use $crate::jni::JNIEnv;

            fn __df_bridge_build(
                env: &mut JNIEnv,
                options: &JByteArray,
                partition: &JByteArray,
            ) -> $crate::JniResult<std::sync::Arc<dyn $crate::datafusion::catalog::TableProvider>>
            {
                let opts: Vec<u8> = if options.is_null() {
                    Vec::new()
                } else {
                    env.convert_byte_array(options)?
                };
                let part: Vec<u8> = if partition.is_null() {
                    Vec::new()
                } else {
                    env.convert_byte_array(partition)?
                };
                let ctx = $crate::BridgeContext::get();
                ($builder)(&ctx, opts.as_slice(), part.as_slice())
            }

            #[export_name = concat!("Java_", $cls, "_providerSchemaIpc")]
            extern "system" fn __df_bridge_provider_schema_ipc<'local>(
                mut env: JNIEnv<'local>,
                _class: JClass<'local>,
                options: JByteArray<'local>,
                partition: JByteArray<'local>,
            ) -> jbyteArray {
                $crate::scan::provider_schema_ipc(&mut env, |env| {
                    __df_bridge_build(env, &options, &partition)
                })
            }

            #[export_name = concat!("Java_", $cls, "_createScan")]
            #[allow(clippy::too_many_arguments)]
            extern "system" fn __df_bridge_create_scan<'local>(
                mut env: JNIEnv<'local>,
                _class: JClass<'local>,
                options: JByteArray<'local>,
                partition: JByteArray<'local>,
                target_partitions: jint,
                batch_size: jint,
                option_keys: JObjectArray<'local>,
                option_values: JObjectArray<'local>,
                projection_columns: JObjectArray<'local>,
                filter_protos: JObjectArray<'local>,
            ) -> jlong {
                $crate::scan::create_scan(
                    &mut env,
                    |env| __df_bridge_build(env, &options, &partition),
                    target_partitions,
                    batch_size,
                    &option_keys,
                    &option_values,
                    &projection_columns,
                    &filter_protos,
                )
            }

            #[export_name = concat!("Java_", $cls, "_partitionCount")]
            extern "system" fn __df_bridge_partition_count<'local>(
                mut env: JNIEnv<'local>,
                _class: JClass<'local>,
                handle: jlong,
            ) -> jint {
                $crate::scan::partition_count(&mut env, handle)
            }

            #[export_name = concat!("Java_", $cls, "_executeStreamPartition")]
            extern "system" fn __df_bridge_execute_stream_partition<'local>(
                mut env: JNIEnv<'local>,
                _class: JClass<'local>,
                handle: jlong,
                partition: jint,
                ffi_stream_addr: jlong,
            ) {
                $crate::scan::execute_stream_partition(&mut env, handle, partition, ffi_stream_addr)
            }

            #[export_name = concat!("Java_", $cls, "_executeStream")]
            extern "system" fn __df_bridge_execute_stream<'local>(
                mut env: JNIEnv<'local>,
                _class: JClass<'local>,
                handle: jlong,
                ffi_stream_addr: jlong,
            ) {
                $crate::scan::execute_stream(&mut env, handle, ffi_stream_addr)
            }

            #[export_name = concat!("Java_", $cls, "_closeScan")]
            extern "system" fn __df_bridge_close_scan<'local>(
                mut env: JNIEnv<'local>,
                _class: JClass<'local>,
                handle: jlong,
            ) {
                $crate::scan::close_scan(&mut env, handle)
            }
        };
    };
}
