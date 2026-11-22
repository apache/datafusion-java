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
//! `TableProvider`s. Legacy providers retain full-schema scans. Providers overriding
//! `scanWithArgs` receive DataFusion's planning request and supply a lazy stream; the adapter
//! does not interpret source predicates or choose source-specific access paths.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchReader};
use datafusion::catalog::Session;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_proto::logical_plan::{to_proto::serialize_expr, DefaultLogicalExtensionCodec};
use futures::stream::StreamExt;
use jni::objects::{GlobalRef, JIntArray, JObject, JObjectArray, JStaticMethodID};
use jni::signature::{Primitive, ReturnType};
use jni::sys::{jlong, jvalue};
use jni::JNIEnv;
use prost::Message;

use crate::jni_util::jthrowable_to_string;
use crate::proto_gen::{TableScanOptionsProto, TableScanProjectionProto};

pub(crate) struct JavaTableProvider {
    pub(crate) name: String,
    pub(crate) schema: SchemaRef,
    pub(crate) source_global_ref: Arc<GlobalRef>,
    pub(crate) bridge_class: Arc<GlobalRef>,
    pub(crate) invoke_method: JStaticMethodID,
    pub(crate) plan_method: JStaticMethodID,
    pub(crate) pushdown_method: JStaticMethodID,
}

impl fmt::Debug for JavaTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JavaTableProvider")
            .field("name", &self.name)
            .field("schema", &self.schema)
            .finish()
    }
}

fn jni_error(error: jni::errors::Error) -> DataFusionError {
    DataFusionError::Execution(format!("TableProvider JNI call failed: {error}"))
}

fn checked_call<T>(env: &mut JNIEnv<'_>, result: jni::errors::Result<T>, name: &str) -> Result<T> {
    if env.exception_check().map_err(jni_error)? {
        let throwable = env.exception_occurred().map_err(jni_error)?;
        env.exception_clear().map_err(jni_error)?;
        return Err(DataFusionError::Execution(jthrowable_to_string(
            env,
            &throwable,
            "TableProvider",
            name,
        )));
    }
    result.map_err(jni_error)
}

fn filter_array<'local>(
    env: &mut JNIEnv<'local>,
    filters: &[Vec<u8>],
) -> Result<JObjectArray<'local>> {
    let array = env
        .new_object_array(filters.len() as i32, "[B", JObject::null())
        .map_err(jni_error)?;
    for (index, filter) in filters.iter().enumerate() {
        let bytes = env.byte_array_from_slice(filter).map_err(jni_error)?;
        env.set_object_array_element(&array, index as i32, &bytes)
            .map_err(jni_error)?;
        env.delete_local_ref(bytes).map_err(jni_error)?;
    }
    Ok(array)
}

#[async_trait]
impl TableProvider for JavaTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let mut capabilities = vec![TableProviderFilterPushDown::Unsupported; filters.len()];
        if filters.is_empty() {
            return Ok(capabilities);
        }
        let codec = DefaultLogicalExtensionCodec {};
        let mut encoded = Vec::new();
        let mut positions = Vec::new();
        for (index, filter) in filters.iter().enumerate() {
            // Expressions that cannot cross this protobuf boundary remain in DataFusion.
            if let Ok(proto) = serialize_expr(filter, &codec) {
                encoded.push(proto.encode_to_vec());
                positions.push(index);
            }
        }
        if encoded.is_empty() {
            return Ok(capabilities);
        }
        let mut env = crate::jvm().attach_current_thread().map_err(jni_error)?;
        let filters = filter_array(&mut env, &encoded)?;
        let args = [
            jvalue {
                l: self.source_global_ref.as_obj().as_raw(),
            },
            jvalue {
                l: filters.as_raw(),
            },
        ];
        let call = unsafe {
            env.call_static_method_unchecked(
                self.bridge_class.as_ref(),
                self.pushdown_method,
                ReturnType::Object,
                &args,
            )
        };
        let values = JIntArray::from(
            checked_call(&mut env, call, &self.name)?
                .l()
                .map_err(jni_error)?,
        );
        let mut codes = vec![0; encoded.len()];
        env.get_int_array_region(&values, 0, &mut codes)
            .map_err(jni_error)?;
        for (position, code) in positions.into_iter().zip(codes) {
            capabilities[position] = match code {
                1 => TableProviderFilterPushDown::Unsupported,
                2 => TableProviderFilterPushDown::Inexact,
                3 => TableProviderFilterPushDown::Exact,
                _ => {
                    return Err(DataFusionError::Execution(format!(
                        "Java TableProvider '{}' returned invalid filter capability {code}",
                        self.name
                    )))
                }
            };
        }
        Ok(capabilities)
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = match projection {
            Some(p) => Arc::new(self.schema.project(p)?),
            None => Arc::clone(&self.schema),
        };
        let options = TableScanOptionsProto {
            projection: projection.map(|indices| TableScanProjectionProto {
                indices: indices.iter().map(|index| *index as u32).collect(),
            }),
            // A scan limit is an optional hint. If it cannot fit a Java long,
            // omit the hint rather than rejecting a query DataFusion can execute.
            limit: limit.and_then(|value| i64::try_from(value).ok()),
        };
        let codec = DefaultLogicalExtensionCodec {};
        let encoded = filters
            .iter()
            .map(|filter| serialize_expr(filter, &codec).map(|proto| proto.encode_to_vec()))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| {
                DataFusionError::Execution(format!("Cannot encode scan filter: {error}"))
            })?;
        let mut env = crate::jvm().attach_current_thread().map_err(jni_error)?;
        let options = env
            .byte_array_from_slice(&options.encode_to_vec())
            .map_err(jni_error)?;
        let filters = filter_array(&mut env, &encoded)?;
        let args = [
            jvalue {
                l: self.source_global_ref.as_obj().as_raw(),
            },
            jvalue {
                l: options.as_raw(),
            },
            jvalue {
                l: filters.as_raw(),
            },
        ];
        let call = unsafe {
            env.call_static_method_unchecked(
                self.bridge_class.as_ref(),
                self.plan_method,
                ReturnType::Object,
                &args,
            )
        };
        let scan = checked_call(&mut env, call, &self.name)?
            .l()
            .map_err(jni_error)?;
        let scan_global_ref = Arc::new(env.new_global_ref(scan).map_err(jni_error)?);
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
            scan_global_ref,
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
    scan_global_ref: Arc<GlobalRef>,
    bridge_class: Arc<GlobalRef>,
    invoke_method: JStaticMethodID,
    plan_properties: Arc<PlanProperties>,
}

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

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    /// A leaf scan: the Java side owns the row production, so there are no
    /// `PhysicalExpr`s on this node for the visitor to descend into.
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
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
        // the synchronous `executePlan` call. Subsequent polls of the
        // returned stream do not need this attachment, because the FFI release /
        // get_next callbacks installed by arrow-java's `Data.exportArrayStream`
        // self-attach to the JVM via the global `JavaVM` set in our `JNI_OnLoad`.
        let mut env = crate::jvm()
            .attach_current_thread()
            .map_err(|e| DataFusionError::Execution(format!("JNI attach failed: {}", e)))?;

        let source_jobj = self.scan_global_ref.as_obj();
        let call_args: [jvalue; 3] = [
            jvalue {
                l: source_jobj.as_raw(),
            },
            jvalue {
                i: partition as i32,
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

        checked_call(&mut env, call_result, &self.name)?;

        // 4. Reclaim the FFI struct and import as a RecordBatchReader.
        let ffi_stream: FFI_ArrowArrayStream = *ffi_box;
        let reader = ArrowArrayStreamReader::try_new(ffi_stream)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        // 5. A plan can push projection into its reader or return the registered full schema,
        // as the default Java scanWithArgs implementation does for legacy providers.
        let reader_schema = reader.schema();
        let projection = if reader_schema == self.projected_schema {
            None
        } else if reader_schema == self.full_schema {
            self.projection.clone()
        } else {
            return Err(DataFusionError::Execution(format!(
                "Java TableProvider '{}' returned schema {:?}; expected projected schema {:?} or registered schema {:?}",
                self.name, reader_schema, self.projected_schema, self.full_schema
            )));
        };

        // 6. Wrap as a Stream and (if a projection is set) project each batch.
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
