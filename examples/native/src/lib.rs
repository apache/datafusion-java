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

//! Example cdylib that produces a small DataFusion `MemTable` wrapped as an
//! `FFI_TableProvider`, returned to the JVM as a `jlong` (the raw boxed
//! pointer). The JVM example uses `SessionContext.registerFfiTable(name, ptr)`
//! to install the provider on a DataFusion session and runs SQL against it.
//!
//! The same pattern is what domain bridges (Rerun, HDF5, custom Iceberg) use
//! to expose their TableProviders to DataFusion-Java — and, transitively, to
//! Spark via the connector-core DataSource V2 plumbing.
//!
//! ## Options wire format
//!
//! `createMemTableProvider` accepts an opaque `byte[]` that the JVM-side
//! `ExampleFfiProviderFactory.encodeOptions` produces. Layout (little-endian):
//!
//! ```text
//! [u32 name_prefix_len][name_prefix UTF-8 bytes][u32 num_rows][u32 num_batches]
//!     [u32 num_partitions][u8 shared_scan]    <- optional trailing fields
//! ```
//!
//! Empty/`null` bytes decode as all defaults: `name_prefix="row"`, `num_rows=4`,
//! `num_batches=1`, `num_partitions=1`, `shared_scan=false`. The trailing
//! fields are optional so blobs from older encoders keep decoding. The
//! `shared_scan` flag is consumed JVM-side (`ExampleFfiProviderFactory.sharedScan`);
//! this decoder carries it only so one blob format serves both sides. Real
//! bridges use a real proto schema here; this example hand-rolls the encoding
//! to keep the wire layer obvious.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;
use datafusion::execution::TaskContextProvider;
use datafusion::prelude::SessionContext;
use datafusion_ffi::execution::FFI_TaskContextProvider;
use datafusion_ffi::table_provider::FFI_TableProvider;
use jni::objects::{JByteArray, JClass};
use jni::sys::jlong;
use jni::JNIEnv;
use tokio::runtime::{Handle, Runtime};

/// Tokio runtime that the FFI provider is anchored to. Shared across calls
/// for the lifetime of the cdylib so successive `createMemTableProvider`
/// invocations don't spawn fresh runtimes.
fn runtime() -> &'static Handle {
    use std::sync::OnceLock;
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| Runtime::new().expect("tokio runtime init failed"))
        .handle()
}

/// Throwaway `SessionContext` used only to obtain a `TaskContextProvider`
/// for `FFI_TableProvider::new`. The example does not register anything on it.
fn host_session_context() -> &'static Arc<SessionContext> {
    use std::sync::OnceLock;
    static CTX: OnceLock<Arc<SessionContext>> = OnceLock::new();
    CTX.get_or_init(|| Arc::new(SessionContext::new()))
}

#[derive(Debug)]
struct Options {
    name_prefix: String,
    num_rows: u32,
    num_batches: u32,
    num_partitions: u32,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            name_prefix: "row".to_string(),
            num_rows: 4,
            num_batches: 1,
            num_partitions: 1,
        }
    }
}

fn decode_options(bytes: &[u8]) -> Result<Options, Box<dyn std::error::Error + Send + Sync>> {
    if bytes.is_empty() {
        return Ok(Options::default());
    }
    if bytes.len() < 4 {
        return Err("options blob too short for name_prefix length prefix".into());
    }
    let name_len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let name_end = 4 + name_len;
    if bytes.len() < name_end + 8 {
        return Err("options blob truncated: missing name_prefix bytes or trailing ints".into());
    }
    let name_prefix = std::str::from_utf8(&bytes[4..name_end])
        .map_err(|e| format!("name_prefix is not valid UTF-8: {e}"))?
        .to_string();
    let num_rows = u32::from_le_bytes(bytes[name_end..name_end + 4].try_into().unwrap());
    let num_batches = u32::from_le_bytes(bytes[name_end + 4..name_end + 8].try_into().unwrap());
    if num_rows == 0 || num_batches == 0 {
        return Err("num_rows and num_batches must both be > 0".into());
    }
    // Optional trailing fields (older encoders omit them): num_partitions,
    // then the shared_scan flag byte, which only the JVM side interprets.
    let num_partitions = if bytes.len() >= name_end + 12 {
        u32::from_le_bytes(bytes[name_end + 8..name_end + 12].try_into().unwrap())
    } else {
        1
    };
    if num_partitions == 0 {
        return Err("num_partitions must be > 0".into());
    }
    Ok(Options {
        name_prefix,
        num_rows,
        num_batches,
        num_partitions,
    })
}

/// Build the example schema + a multi-batch in-memory table sized per `opts`.
/// Row `r` in batch `b` gets `id = b * num_rows + r`, `name = "<prefix><id>"`,
/// `value = id * 1.5` (with `value` left null for every fourth row so the demo
/// still exercises null handling).
fn build_mem_table(
    opts: &Options,
) -> Result<Arc<MemTable>, Box<dyn std::error::Error + Send + Sync>> {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]));

    let mut batches = Vec::with_capacity(opts.num_batches as usize);
    for b in 0..opts.num_batches {
        let mut ids = Vec::with_capacity(opts.num_rows as usize);
        let mut names: Vec<Option<String>> = Vec::with_capacity(opts.num_rows as usize);
        let mut values: Vec<Option<f64>> = Vec::with_capacity(opts.num_rows as usize);
        for r in 0..opts.num_rows {
            let id = (b as i64) * (opts.num_rows as i64) + (r as i64);
            ids.push(id);
            names.push(Some(format!("{}{}", opts.name_prefix, id)));
            values.push(if id % 4 == 3 {
                None
            } else {
                Some(id as f64 * 1.5)
            });
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(Float64Array::from(values)),
            ],
        )?;
        batches.push(batch);
    }

    // Distribute the batches round-robin across `num_partitions` MemTable
    // partitions. With num_partitions=1 the example stays single-partition;
    // larger values give the Spark connector's shared-scan mode real
    // DataFusion-native partitions to map tasks onto. Partitions beyond the
    // batch count stay empty — DataFusion handles empty partitions fine.
    let mut partitions: Vec<Vec<RecordBatch>> = vec![Vec::new(); opts.num_partitions as usize];
    for (i, batch) in batches.into_iter().enumerate() {
        partitions[i % opts.num_partitions as usize].push(batch);
    }
    Ok(Arc::new(MemTable::try_new(schema, partitions)?))
}

/// JNI entry point: decode the options blob, build a `MemTable` accordingly,
/// wrap it in an `FFI_TableProvider`, return the raw boxed pointer as a `jlong`.
/// Ownership of the boxed FFI transfers to the caller — the matching
/// `Box::from_raw` is performed by `SessionContext.registerFfiTable` on the
/// consumer side.
#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_examples_FfiTableProviderExampleNative_createMemTableProvider<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    options_bytes: JByteArray<'local>,
) -> jlong {
    let result: Result<jlong, Box<dyn std::error::Error + Send + Sync>> = (|| {
        let bytes: Vec<u8> = if options_bytes.is_null() {
            Vec::new()
        } else {
            env.convert_byte_array(&options_bytes)
                .map_err(|e| format!("failed to read options byte[] from JVM: {e}"))?
        };
        let opts = decode_options(&bytes)?;

        let mem_table = build_mem_table(&opts)?;
        let provider: Arc<dyn TableProvider> = mem_table;

        let ctx_provider: Arc<dyn TaskContextProvider> =
            Arc::clone(host_session_context()) as Arc<dyn TaskContextProvider>;
        let ffi_task_ctx = FFI_TaskContextProvider::from(&ctx_provider);
        let ffi = FFI_TableProvider::new(
            provider,
            /*can_support_pushdown_filters=*/ true,
            Some(runtime().clone()),
            ffi_task_ctx,
            /*logical_codec=*/ None,
        );
        Ok(Box::into_raw(Box::new(ffi)) as jlong)
    })();

    match result {
        Ok(ptr) => ptr,
        Err(err) => {
            let _ = env.throw_new("java/lang/RuntimeException", err.to_string());
            0
        }
    }
}

/// Drop a previously-created FFI_TableProvider whose pointer was NOT handed
/// off to `registerFfiTable`. Exposed for symmetry — callers that pass the
/// pointer to `registerFfiTable` must NOT also call this; ownership has
/// already transferred.
#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_examples_FfiTableProviderExampleNative_dropProvider<
    'local,
>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    ffi_ptr: jlong,
) {
    if ffi_ptr != 0 {
        unsafe {
            drop(Box::from_raw(ffi_ptr as *mut FFI_TableProvider));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(prefix: &str, num_rows: u32, num_batches: u32) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(prefix.len() as u32).to_le_bytes());
        buf.extend_from_slice(prefix.as_bytes());
        buf.extend_from_slice(&num_rows.to_le_bytes());
        buf.extend_from_slice(&num_batches.to_le_bytes());
        buf
    }

    #[test]
    fn empty_bytes_decodes_to_defaults() {
        let o = decode_options(&[]).unwrap();
        assert_eq!(o.name_prefix, "row");
        assert_eq!(o.num_rows, 4);
        assert_eq!(o.num_batches, 1);
        assert_eq!(o.num_partitions, 1);
    }

    #[test]
    fn roundtrip_decodes_options() {
        let o = decode_options(&encode("user", 5, 3)).unwrap();
        assert_eq!(o.name_prefix, "user");
        assert_eq!(o.num_rows, 5);
        assert_eq!(o.num_batches, 3);
    }

    #[test]
    fn old_blob_without_trailing_fields_defaults_partitions_to_one() {
        let o = decode_options(&encode("user", 5, 3)).unwrap();
        assert_eq!(o.num_partitions, 1);
    }

    #[test]
    fn trailing_fields_decode_num_partitions_and_ignore_flag_byte() {
        let mut buf = encode("user", 5, 8);
        buf.extend_from_slice(&4u32.to_le_bytes());
        buf.push(1); // shared_scan flag: JVM-side only
        let o = decode_options(&buf).unwrap();
        assert_eq!(o.num_partitions, 4);
    }

    #[test]
    fn zero_partitions_rejected() {
        let mut buf = encode("user", 5, 8);
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.push(0);
        assert!(decode_options(&buf).is_err());
    }

    #[test]
    fn batches_distribute_round_robin_across_partitions() {
        let opts = Options {
            name_prefix: "u".to_string(),
            num_rows: 2,
            num_batches: 5,
            num_partitions: 3,
        };
        let table = build_mem_table(&opts).unwrap();
        // MemTable has no partition accessor; verify via scan output partitioning.
        use datafusion::catalog::TableProvider;
        let ctx = SessionContext::new();
        let rt = Runtime::new().unwrap();
        let plan = rt
            .block_on(async { table.scan(&ctx.state(), None, &[], None).await })
            .unwrap();
        assert_eq!(plan.properties().output_partitioning().partition_count(), 3);
    }

    #[test]
    fn build_table_has_expected_schema() {
        let opts = Options {
            name_prefix: "user".to_string(),
            num_rows: 5,
            num_batches: 3,
            num_partitions: 1,
        };
        let table = build_mem_table(&opts).unwrap();
        let schema = table.schema();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(2).name(), "value");
    }

    #[test]
    fn rejects_zero_counts() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&3u32.to_le_bytes());
        buf.extend_from_slice(b"abc");
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(&1u32.to_le_bytes());
        assert!(decode_options(&buf).is_err());
    }
}
