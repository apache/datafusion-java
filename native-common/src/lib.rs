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

//! JNI plumbing shared by this workspace's cdylibs (`datafusion-jni` and the
//! Spark connector helper): the error-to-Java-exception mapping, the
//! per-cdylib Tokio runtime singleton, and the async-stream-to-
//! `FFI_ArrowArrayStream` bridge.
//!
//! Each cdylib statically links its own copy of this rlib, so [`runtime`] is
//! a per-cdylib singleton -- exactly the behaviour each crate had when this
//! code lived inline. Nothing here is exported with `#[no_mangle]`, so
//! linking this crate into several cdylibs loaded in one JVM cannot collide.

pub mod errors;

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::OnceLock;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatchReader;
use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;
use tokio::runtime::{Handle, Runtime};

static RT: OnceLock<Runtime> = OnceLock::new();

/// The cdylib-wide Tokio runtime.
pub fn runtime() -> &'static Runtime {
    runtime_with_init(|_| {})
}

/// Same singleton as [`runtime`], with a hook that runs exactly once, when
/// the runtime is created. `datafusion-jni` uses it to install its
/// runtime-metrics accumulator so the sampling baseline coincides with
/// runtime start; every later call (either entry point) returns the existing
/// runtime without invoking the hook.
pub fn runtime_with_init(init: impl FnOnce(&Handle)) -> &'static Runtime {
    RT.get_or_init(|| {
        let rt = Runtime::new().expect("failed to create Tokio runtime");
        init(rt.handle());
        rt
    })
}

/// Bridges DataFusion's async [`SendableRecordBatchStream`] to the synchronous
/// [`RecordBatchReader`] interface that `FFI_ArrowArrayStream` (and therefore
/// the Java `ArrowReader`) consumes. Each call to `next()` drives one
/// `runtime().block_on(stream.next())`, so memory pressure stays bounded by the
/// executor pipeline plus a single in-flight batch.
pub struct StreamingReader {
    pub schema: SchemaRef,
    pub stream: SendableRecordBatchStream,
}

impl Iterator for StreamingReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Arrow's C ABI invokes this iterator through FFI_ArrowArrayStream's
        // vtable, outside the JNI handler's try_unwrap_or_throw guard. A panic
        // here (buggy UDF, arrow cast that panics, runtime poison) would
        // unwind across C/FFI -- undefined behaviour. Catch it and surface as
        // an ArrowError so the Java side sees a normal exception instead.
        let next = catch_unwind(AssertUnwindSafe(|| runtime().block_on(self.stream.next())));
        match next {
            Ok(item) => item.map(|r| r.map_err(|e| ArrowError::ExternalError(Box::new(e)))),
            Err(panic) => {
                let msg = if let Some(s) = panic.downcast_ref::<String>() {
                    s.clone()
                } else if let Some(s) = panic.downcast_ref::<&str>() {
                    (*s).to_string()
                } else {
                    "rust panic with non-string payload".to_string()
                };
                Some(Err(ArrowError::ExternalError(
                    format!("panic in DataFrame stream: {msg}").into(),
                )))
            }
        }
    }
}

impl RecordBatchReader for StreamingReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
