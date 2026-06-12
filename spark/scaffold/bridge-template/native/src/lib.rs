//! Native side of the `__FORMAT__` Spark bridge.
//!
//! `export_bridge!` generates the whole JNI surface for
//! `__PKG__.BridgeNative`; the only code you own is [`build_provider`],
//! which turns the option/partition bytes your JVM factory encoded into a
//! concrete `TableProvider`. Everything downstream — type widening, session
//! construction, projection, pushed filters, planning, partition streams —
//! is the SDK's job.

use std::sync::Arc;

use datafusion_spark_bridge::datafusion::arrow::array::{Int64Array, StringArray};
use datafusion_spark_bridge::datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion_spark_bridge::datafusion::arrow::record_batch::RecordBatch;
use datafusion_spark_bridge::datafusion::catalog::TableProvider;
use datafusion_spark_bridge::datafusion::datasource::MemTable;
use datafusion_spark_bridge::options::decode_options;
use datafusion_spark_bridge::{export_bridge, BridgeContext, JniResult};

/// Build the provider for one scan.
///
/// `options` is whatever the JVM factory's `encodeOptions` produced — with
/// the default factory that is the connector's `OptionsCodec` format, decoded
/// below into a string map. `partition` is the per-task payload from
/// `listPartitions` (empty for the schema probe, for shared-scan mode, and
/// for the default single-partition layout).
///
/// TODO: replace the demo `MemTable` with your real `TableProvider`. For
/// async construction (remote catalogs, object stores), use
/// `ctx.block_on(...)`.
fn build_provider(
    _ctx: &BridgeContext,
    options: &[u8],
    _partition: &[u8],
) -> JniResult<Arc<dyn TableProvider>> {
    let opts = decode_options(options)?;
    let rows: i64 = match opts.get("rows") {
        Some(v) => v
            .parse()
            .map_err(|e| format!("option 'rows' is not an integer: {e}"))?,
        None => 3,
    };

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("greeting", DataType::Utf8, false),
    ]));
    let ids = Int64Array::from_iter_values(0..rows);
    let greetings =
        StringArray::from_iter_values((0..rows).map(|i| format!("hello from __FORMAT__ #{i}")));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(greetings)])?;

    Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
}

export_bridge! {
    jni_class: "__JNI_CLASS__",
    build_provider: build_provider,
}
