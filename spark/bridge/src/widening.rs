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

//! Kernel-level Arrow type widening for Spark consumption.
//!
//! Spark 3.5's `ArrowColumnVector` has no accessor for unsigned ints, Time*,
//! Float16, or non-microsecond Timestamp. The widening machinery here wraps
//! an inner `TableProvider` with one that exposes a "widened" schema —
//! UInt*→Int wider, Float16→Float32, Time*→Int wider, Timestamp(*, tz)→
//! Timestamp(Microsecond, tz), recursing into List/LargeList/FixedSizeList
//! children — and applies `arrow::compute::cast` to each produced
//! RecordBatch column-wise. No SQL, no SessionContext, no view machinery.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::stream::StreamExt;

/// Compute the cast-target DataType for an Arrow type not directly readable
/// by Spark's `ArrowColumnVector`. Returns `None` if the type passes through.
pub fn arrow_cast_widening(dt: &DataType) -> Option<DataType> {
    match dt {
        DataType::UInt8 => Some(DataType::Int16),
        DataType::UInt16 => Some(DataType::Int32),
        DataType::UInt32 => Some(DataType::Int64),
        // UInt64 → Int64: lossy for values ≥ 2^63. Documented in REARCHITECTURE.md.
        DataType::UInt64 => Some(DataType::Int64),
        DataType::Float16 => Some(DataType::Float32),
        DataType::Time32(_) => Some(DataType::Int32),
        DataType::Time64(_) => Some(DataType::Int64),
        DataType::Timestamp(unit, tz) => {
            if *unit == TimeUnit::Microsecond {
                None
            } else {
                Some(DataType::Timestamp(TimeUnit::Microsecond, tz.clone()))
            }
        }
        DataType::List(field) => arrow_cast_widening(field.data_type())
            .map(|inner| DataType::List(widened_child(field, inner))),
        DataType::LargeList(field) => arrow_cast_widening(field.data_type())
            .map(|inner| DataType::LargeList(widened_child(field, inner))),
        // Spark 3.5's ArrowColumnVector cannot read FixedSizeList at all, so
        // always convert it to a (variable) List — which Spark maps to
        // ArrayType — widening the child element type when needed too.
        DataType::FixedSizeList(field, _size) => {
            let child = match arrow_cast_widening(field.data_type()) {
                Some(inner) => widened_child(field, inner),
                None => Arc::clone(field),
            };
            Some(DataType::List(child))
        }
        _ => None,
    }
}

fn widened_child(field: &Arc<Field>, new_type: DataType) -> Arc<Field> {
    Arc::new(Field::new(field.name(), new_type, field.is_nullable()))
}

/// Build the widened schema by walking inner fields and replacing types.
/// Returns the widened schema plus per-column target types (None where no cast).
fn widened_schema(inner: &ArrowSchema) -> (SchemaRef, Vec<Option<DataType>>) {
    let mut fields = Vec::with_capacity(inner.fields().len());
    let mut targets = Vec::with_capacity(inner.fields().len());
    for f in inner.fields() {
        match arrow_cast_widening(f.data_type()) {
            Some(target) => {
                fields.push(Arc::new(Field::new(
                    f.name(),
                    target.clone(),
                    f.is_nullable(),
                )));
                targets.push(Some(target));
            }
            None => {
                fields.push(Arc::clone(f));
                targets.push(None);
            }
        }
    }
    (Arc::new(ArrowSchema::new(fields)), targets)
}

/// TableProvider wrapping an inner provider, exposing a widened schema and
/// emitting RecordBatches whose columns have been cast to the widened types.
#[derive(Debug)]
pub struct WideningTableProvider {
    inner: Arc<dyn TableProvider>,
    widened: SchemaRef,
    /// Targets indexed by the inner-schema column position; `None` = pass through.
    targets: Vec<Option<DataType>>,
}

impl WideningTableProvider {
    pub fn new(inner: Arc<dyn TableProvider>) -> Self {
        let (widened, targets) = widened_schema(&inner.schema());
        Self {
            inner,
            widened,
            targets,
        }
    }
}

#[async_trait]
impl TableProvider for WideningTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.widened)
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let inner_plan = self.inner.scan(session, projection, filters, limit).await?;
        let (projected_widened, projected_targets) = match projection {
            Some(idxs) => {
                let fields: Vec<Arc<Field>> = idxs
                    .iter()
                    .map(|i| Arc::clone(&self.widened.fields()[*i]))
                    .collect();
                let targets: Vec<Option<DataType>> =
                    idxs.iter().map(|i| self.targets[*i].clone()).collect();
                (Arc::new(ArrowSchema::new(fields)) as SchemaRef, targets)
            }
            None => (Arc::clone(&self.widened), self.targets.clone()),
        };
        Ok(Arc::new(WideningExec::new(
            inner_plan,
            projected_widened,
            projected_targets,
        )))
    }
}

/// ExecutionPlan that runs the inner plan and casts each output RecordBatch
/// column-wise per the supplied targets. Pure stream-map wrapper; no
/// buffering, no internal state.
pub struct WideningExec {
    inner: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    /// One entry per output column; `None` = pass through.
    targets: Vec<Option<DataType>>,
    properties: Arc<PlanProperties>,
}

impl WideningExec {
    fn new(
        inner: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        targets: Vec<Option<DataType>>,
    ) -> Self {
        let inner_props = inner.properties();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            inner_props.partitioning.clone(),
            inner_props.emission_type,
            inner_props.boundedness,
        ));
        Self {
            inner,
            schema,
            targets,
            properties,
        }
    }
}

impl fmt::Debug for WideningExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WideningExec")
            .field("schema", &self.schema)
            .field("targets", &self.targets)
            .finish()
    }
}

impl DisplayAs for WideningExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cast_count = self.targets.iter().filter(|t| t.is_some()).count();
        write!(f, "WideningExec: casts={cast_count}")
    }
}

impl ExecutionPlan for WideningExec {
    fn name(&self) -> &str {
        "WideningExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "WideningExec::with_new_children expects exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(WideningExec::new(
            children.into_iter().next().unwrap(),
            Arc::clone(&self.schema),
            self.targets.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let inner_stream = self.inner.execute(partition, context)?;
        let schema = Arc::clone(&self.schema);
        let targets = self.targets.clone();
        let mapped = inner_stream.map(move |batch_res| match batch_res {
            Err(e) => Err(e),
            Ok(batch) => cast_batch(&batch, &schema, &targets),
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            mapped,
        )))
    }
}

fn cast_batch(
    batch: &RecordBatch,
    out_schema: &SchemaRef,
    targets: &[Option<DataType>],
) -> Result<RecordBatch> {
    if batch.num_columns() != targets.len() {
        return Err(DataFusionError::Internal(format!(
            "WideningExec: produced batch has {} columns, expected {}",
            batch.num_columns(),
            targets.len()
        )));
    }
    let mut new_cols = Vec::with_capacity(batch.num_columns());
    for (col, target) in batch.columns().iter().zip(targets.iter()) {
        match target {
            Some(t) => new_cols.push(cast(col, t).map_err(DataFusionError::from)?),
            None => new_cols.push(Arc::clone(col)),
        }
    }
    RecordBatch::try_new(Arc::clone(out_schema), new_cols).map_err(DataFusionError::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsigned_ints_widen_to_signed_wider() {
        assert_eq!(arrow_cast_widening(&DataType::UInt8), Some(DataType::Int16));
        assert_eq!(
            arrow_cast_widening(&DataType::UInt16),
            Some(DataType::Int32)
        );
        assert_eq!(
            arrow_cast_widening(&DataType::UInt32),
            Some(DataType::Int64)
        );
        assert_eq!(
            arrow_cast_widening(&DataType::UInt64),
            Some(DataType::Int64)
        );
    }

    #[test]
    fn float16_widens_to_float32() {
        assert_eq!(
            arrow_cast_widening(&DataType::Float16),
            Some(DataType::Float32)
        );
    }

    #[test]
    fn time_widens_to_int() {
        assert_eq!(
            arrow_cast_widening(&DataType::Time32(TimeUnit::Millisecond)),
            Some(DataType::Int32)
        );
        assert_eq!(
            arrow_cast_widening(&DataType::Time64(TimeUnit::Nanosecond)),
            Some(DataType::Int64)
        );
    }

    #[test]
    fn timestamp_normalizes_unit_preserving_tz() {
        let ns = DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC")));
        assert_eq!(
            arrow_cast_widening(&ns),
            Some(DataType::Timestamp(
                TimeUnit::Microsecond,
                Some(Arc::from("UTC"))
            ))
        );
        let us_no_tz = DataType::Timestamp(TimeUnit::Microsecond, None);
        assert_eq!(arrow_cast_widening(&us_no_tz), None);
    }

    #[test]
    fn list_recurses_into_children() {
        let inner_field = Arc::new(Field::new("item", DataType::UInt16, true));
        let list_ty = DataType::List(inner_field);
        let widened = arrow_cast_widening(&list_ty).expect("should widen");
        match widened {
            DataType::List(field) => assert_eq!(field.data_type(), &DataType::Int32),
            other => panic!("expected List, got {other:?}"),
        }
    }

    #[test]
    fn signed_int_passes_through() {
        assert_eq!(arrow_cast_widening(&DataType::Int32), None);
        assert_eq!(arrow_cast_widening(&DataType::Utf8), None);
    }
}
