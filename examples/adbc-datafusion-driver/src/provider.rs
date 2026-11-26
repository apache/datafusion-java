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

//! The "custom" `TableProvider`s this example exposes.
//!
//! In a real deployment this is *your* provider crate -- reading your store,
//! your format, your catalog. Here they are fixed tables so the example is
//! self-contained; the only thing that matters for the integration is that they
//! are ordinary [`datafusion::catalog::TableProvider`]s. Execution is delegated
//! to in-memory [`MemTable`]s; swap the `scan` for your real source and the rest
//! of the pipeline (ADBC driver, Spark connector) is unchanged.
//!
//! Two tables:
//!
//! - [`ExampleTableProvider`] (`example`) -- two Spark-native columns across
//!   three partitions. Keeps the partitioned-execution system test simple and
//!   lets the connector use its preferred Substrait wire.
//! - [`TypesTableProvider`] (`types`) -- a schema spanning both categories the
//!   Spark connector's `SchemaConverter` must handle, so the end-to-end test
//!   covers them with known values and no external server:
//!     - *directly representable* (pass through to `ArrowColumnVector`):
//!       `Int64`, `Utf8`, `Binary`, `List<Struct<..>>`;
//!     - *cast-required* (source-side `arrow_cast` to a Spark-native layout):
//!       unsigned `UInt16`/`UInt64`, nanosecond `Timestamp`, `Float16`, nested
//!       `List<UInt16>`.
//!   Each row is built as its own self-contained single-row batch (rather than
//!   slicing one batch), so every partition's arrays start at offset 0 -- a
//!   sliced array carries a non-zero offset into shared buffers, which the Arrow
//!   C-data export at the ADBC boundary does not rebase, corrupting
//!   variable-width (binary/list) columns on the Spark side.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BinaryArray, Date64Array, FixedSizeBinaryBuilder, FixedSizeListBuilder, Float16Array,
    Int64Array, LargeListBuilder, ListBuilder, StringArray, StringBuilder, StructBuilder,
    TimestampNanosecondArray, UInt16Array, UInt16Builder, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use half::f16;

/// A custom provider exposing a single fixed table named [`Self::TABLE_NAME`].
#[derive(Debug)]
pub struct ExampleTableProvider {
    inner: MemTable,
}

impl ExampleTableProvider {
    /// The name the provider is registered under; the ADBC client scans this.
    pub const TABLE_NAME: &'static str = "example";

    pub fn new() -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        // Three partitions (one row each) so the table has a non-trivial output
        // partitioning -- this is what makes ADBC execute_partitions /
        // read_partition return more than one partition, exercising distributed
        // reads.
        let rows = [(1_i64, "alice"), (2, "bob"), (3, "carol")];
        let partitions: Vec<Vec<RecordBatch>> = rows
            .iter()
            .map(|(id, name)| {
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(vec![*id])),
                        Arc::new(StringArray::from(vec![*name])),
                    ],
                )
                .expect("example record batch");
                vec![batch]
            })
            .collect();
        let inner = MemTable::try_new(schema, partitions).expect("example in-memory table");
        Self { inner }
    }
}

impl Default for ExampleTableProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TableProvider for ExampleTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // A real provider builds its own ExecutionPlan here, honoring the
        // pushed projection / filters / limit. We delegate to MemTable.
        self.inner.scan(state, projection, filters, limit).await
    }
}

/// One row of the [`TypesTableProvider`] table, as plain Rust values.
struct Row {
    id: i64,
    name: &'static str,
    payload: &'static [u8],
    attrs: &'static [(&'static str, &'static str)],
    channel: u16,
    big: u64,
    /// Nanoseconds since the Unix epoch (no timezone).
    event_time: i64,
    score: f32,
    tags: &'static [u16],
    /// A fixed-size list of two unsigned ints -- Spark cannot read FixedSizeList and must have it
    /// cast to a variable List (with the element widened). Always length 2.
    vec: [u16; 2],
    /// A LargeList of strings -- maps to ArrayType but has no accessor, so it casts to List.
    labels: &'static [&'static str],
    /// FixedSizeBinary(4) -- maps to BinaryType but has no accessor, so it casts to Binary.
    digest: [u8; 4],
    /// Date64 (ms since epoch, day-aligned) -- no accessor, casts to Date32.
    day_ms: i64,
}

// Values are chosen to exercise the widening edges: channel spans past i16::MAX, big includes
// u64 values past i64::MAX (must stay lossless via Decimal(20,0)), and event_time is nanoseconds
// that must rescale to microseconds rather than be relabeled.
const TYPE_ROWS: [Row; 3] = [
    Row {
        id: 1,
        name: "alice",
        payload: &[0x01, 0x02],
        attrs: &[("a", "1")],
        channel: 100,
        big: u64::MAX,
        event_time: 1_600_000_000_000_000_000, // 2020-09-13
        score: 1.5,
        tags: &[1, 2],
        vec: [10, 20],
        labels: &["a", "b"],
        digest: [0x01, 0x02, 0x03, 0x04],
        day_ms: 1_599_955_200_000, // 2020-09-13 (18518 days)
    },
    Row {
        id: 2,
        name: "bob",
        payload: &[],
        attrs: &[],
        channel: 40_000,
        big: 0,
        event_time: 1_610_000_000_000_000_000, // 2021-01-07
        score: 2.5,
        tags: &[],
        vec: [30, 40],
        labels: &[],
        digest: [0x00, 0x00, 0x00, 0x00],
        day_ms: 1_609_977_600_000, // 2021-01-07 (18634 days)
    },
    Row {
        id: 3,
        name: "carol",
        payload: &[0xff, 0xfe],
        attrs: &[("b", "2"), ("c", "3")],
        channel: 65_535,
        big: 9_223_372_036_854_775_808,        // 2^63, past i64::MAX
        event_time: 1_620_000_000_000_000_000, // 2021-05-03
        score: 3.5,
        tags: &[3],
        vec: [50, 60],
        labels: &["c"],
        digest: [0xff, 0xfe, 0xfd, 0xfc],
        day_ms: 1_620_000_000_000, // 2021-05-03 (18750 days)
    },
];

/// A provider whose schema exercises every Arrow type the Spark connector's converter handles.
#[derive(Debug)]
pub struct TypesTableProvider {
    inner: MemTable,
}

impl TypesTableProvider {
    pub const TABLE_NAME: &'static str = "types";

    pub fn new() -> Self {
        // Build the schema once from single-row prototype arrays so the declared types (in
        // particular the nested list/struct types) always match the produced data exactly.
        let proto = row_columns(&TYPE_ROWS[0]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("payload", proto[2].data_type().clone(), true),
            Field::new("attrs", proto[3].data_type().clone(), true),
            Field::new("channel", proto[4].data_type().clone(), true),
            Field::new("big", proto[5].data_type().clone(), true),
            Field::new("event_time", proto[6].data_type().clone(), true),
            Field::new("score", proto[7].data_type().clone(), true),
            Field::new("tags", proto[8].data_type().clone(), true),
            Field::new("vec", proto[9].data_type().clone(), true),
            Field::new("labels", proto[10].data_type().clone(), true),
            Field::new("digest", proto[11].data_type().clone(), true),
            Field::new("day", proto[12].data_type().clone(), true),
        ]));

        // One self-contained single-row batch per partition (see the module docs on why we do
        // not slice a shared batch).
        let partitions: Vec<Vec<RecordBatch>> = TYPE_ROWS
            .iter()
            .map(|row| {
                let batch = RecordBatch::try_new(schema.clone(), row_columns(row))
                    .expect("types record batch");
                vec![batch]
            })
            .collect();
        let inner = MemTable::try_new(schema, partitions).expect("types in-memory table");
        Self { inner }
    }
}

impl Default for TypesTableProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TableProvider for TypesTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }
}

/// Build the single-row column arrays for one [`Row`], in schema order.
fn row_columns(row: &Row) -> Vec<ArrayRef> {
    let mut tags = ListBuilder::new(UInt16Builder::new());
    for t in row.tags {
        tags.values().append_value(*t);
    }
    tags.append(true);

    let mut vec = FixedSizeListBuilder::new(UInt16Builder::new(), row.vec.len() as i32);
    for v in row.vec {
        vec.values().append_value(v);
    }
    vec.append(true);

    let mut labels = LargeListBuilder::new(StringBuilder::new());
    for l in row.labels {
        labels.values().append_value(l);
    }
    labels.append(true);

    let mut digest = FixedSizeBinaryBuilder::new(row.digest.len() as i32);
    digest.append_value(row.digest).expect("digest bytes");

    let attr_fields = vec![
        Field::new("key", DataType::Utf8, true),
        Field::new("val", DataType::Utf8, true),
    ];
    let mut attrs = ListBuilder::new(StructBuilder::from_fields(attr_fields, 0));
    for (key, val) in row.attrs {
        let s = attrs.values();
        s.field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value(key);
        s.field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value(val);
        s.append(true);
    }
    attrs.append(true);

    vec![
        Arc::new(Int64Array::from(vec![row.id])),
        Arc::new(StringArray::from(vec![row.name])),
        Arc::new(BinaryArray::from_iter_values([row.payload])),
        Arc::new(attrs.finish()) as ArrayRef,
        Arc::new(UInt16Array::from(vec![row.channel])),
        Arc::new(UInt64Array::from(vec![row.big])),
        Arc::new(TimestampNanosecondArray::from(vec![row.event_time])),
        Arc::new(Float16Array::from(vec![f16::from_f32(row.score)])),
        Arc::new(tags.finish()) as ArrayRef,
        Arc::new(vec.finish()) as ArrayRef,
        Arc::new(labels.finish()) as ArrayRef,
        Arc::new(digest.finish()) as ArrayRef,
        Arc::new(Date64Array::from(vec![row.day_ms])),
    ]
}
