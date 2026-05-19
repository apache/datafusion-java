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

//! Java-backed [`TableProvider`] implementation.
//!
//! Used by `SessionContext::registerTable` on the Java side to register user-implemented
//! `TableProvider`s. The internal struct here mirrors the role of DataFusion's Rust
//! `TableProvider` trait; it currently only supports a single-partition, no-pushdown scan,
//! with future pushdown and partitioning support tracked as follow-ups.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchReader};
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures::stream::StreamExt;
use jni::objects::{GlobalRef, JStaticMethodID};
use jni::signature::{Primitive, ReturnType};
use jni::sys::{jlong, jvalue};

use crate::jni_util::jthrowable_to_string;

pub(crate) struct JavaTableProvider {
    pub(crate) name: String,
    pub(crate) schema: SchemaRef,
    pub(crate) source_global_ref: Arc<GlobalRef>,
    pub(crate) bridge_class: Arc<GlobalRef>,
    pub(crate) invoke_method: JStaticMethodID,
}

// SAFETY: see the matching unsafe impls on JavaScalarUdf. The GlobalRefs keep
// the Java objects alive; JStaticMethodID points into the class held by
// bridge_class; nothing is mutated after construction.
unsafe impl Send for JavaTableProvider {}
unsafe impl Sync for JavaTableProvider {}

impl fmt::Debug for JavaTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JavaTableProvider")
            .field("name", &self.name)
            .field("schema", &self.schema)
            .finish()
    }
}

#[async_trait]
impl TableProvider for JavaTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = match projection {
            Some(p) => Arc::new(self.schema.project(p)?),
            None => Arc::clone(&self.schema),
        };
        let plan_properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&projected_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Arc::new(JavaScanExec {
            name: self.name.clone(),
            full_schema: Arc::clone(&self.schema),
            projected_schema,
            projection: projection.cloned(),
            source_global_ref: Arc::clone(&self.source_global_ref),
            bridge_class: Arc::clone(&self.bridge_class),
            invoke_method: self.invoke_method,
            plan_properties,
        }))
    }
}

pub(crate) struct JavaScanExec {
    name: String,
    full_schema: SchemaRef,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    source_global_ref: Arc<GlobalRef>,
    bridge_class: Arc<GlobalRef>,
    invoke_method: JStaticMethodID,
    plan_properties: Arc<PlanProperties>,
}

// SAFETY: same reasoning as JavaTableProvider above — GlobalRefs via Arc keep
// Java objects alive; JStaticMethodID is stable; nothing mutated after construction.
unsafe impl Send for JavaScanExec {}
unsafe impl Sync for JavaScanExec {}

impl fmt::Debug for JavaScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JavaScanExec")
            .field("name", &self.name)
            .field("projected_schema", &self.projected_schema)
            .finish()
    }
}

impl DisplayAs for JavaScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "JavaScanExec: name={}", self.name)
    }
}

impl ExecutionPlan for JavaScanExec {
    fn name(&self) -> &str {
        "JavaScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "JavaScanExec has 1 partition; got {}",
                partition
            )));
        }

        // 1. Allocate an empty FFI stream and box it for a stable address.
        let mut ffi_box = Box::new(FFI_ArrowArrayStream::empty());
        let ffi_addr = ffi_box.as_mut() as *mut FFI_ArrowArrayStream as jlong;

        // 2. Attach the JVM and call the bridge.
        //
        // The attachment scope is just this function: we need the JVM attached for
        // the synchronous `invokeTableScan` call. Subsequent polls of the
        // returned stream do not need this attachment, because the FFI release /
        // get_next callbacks installed by arrow-java's `Data.exportArrayStream`
        // self-attach to the JVM via the global `JavaVM` set in our `JNI_OnLoad`.
        let mut env = crate::jvm()
            .attach_current_thread()
            .map_err(|e| DataFusionError::Execution(format!("JNI attach failed: {}", e)))?;

        let source_jobj = self.source_global_ref.as_obj();
        let call_args: [jvalue; 2] = [
            jvalue {
                l: source_jobj.as_raw(),
            },
            jvalue { j: ffi_addr },
        ];

        let call_result = unsafe {
            env.call_static_method_unchecked(
                self.bridge_class.as_ref(),
                self.invoke_method,
                ReturnType::Primitive(Primitive::Void),
                &call_args,
            )
        };

        // 3. Surface any Java exception.
        if env.exception_check().unwrap_or(false) {
            let throwable = env.exception_occurred().map_err(|e| {
                DataFusionError::Execution(format!("exception_occurred failed: {}", e))
            })?;
            env.exception_clear().ok();
            return Err(DataFusionError::Execution(jthrowable_to_string(
                &mut env,
                &throwable,
                "TableProvider",
                &self.name,
            )));
        }

        call_result.map_err(|e| DataFusionError::Execution(format!("JNI call failed: {}", e)))?;

        // 4. Reclaim the FFI struct and import as a RecordBatchReader.
        let ffi_stream: FFI_ArrowArrayStream = *ffi_box;
        let reader = ArrowArrayStreamReader::try_new(ffi_stream)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        // 5. Verify the producer's declared schema matches our registered schema.
        let reader_schema = reader.schema();
        // Schema::PartialEq compares fields AND metadata. If IPC / FFI round-trips
        // ever normalise metadata differently between the registration path and the
        // scan path, switch to comparing `.fields()` only.
        if reader_schema.as_ref() != self.full_schema.as_ref() {
            return Err(DataFusionError::Execution(format!(
                "Java TableProvider '{}' returned schema {:?}; registered schema was {:?}",
                self.name, reader_schema, self.full_schema
            )));
        }

        // 6. Wrap as a Stream and (if a projection is set) project each batch.
        let projection = self.projection.clone();
        let stream = futures::stream::iter(reader).map(move |batch_result| {
            let batch: RecordBatch =
                batch_result.map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            match &projection {
                Some(p) => batch
                    .project(p)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None)),
                None => Ok(batch),
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.projected_schema),
            stream,
        )))
    }
}
