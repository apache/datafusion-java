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

//! Rerun `TableProvider` registration and segment enumeration JNI surface.
//!
//! Two JNI entry points used by the Spark connector. Scan + filter + projection
//! reuse the existing `createDataFrameFromProto` path (the JVM side encodes a
//! `LogicalPlanNode` referencing the registered table) so no new scan JNI is
//! introduced here.
//!
//! - `registerRerunTableNative`: decode a [`RerunTableOptions`] envelope,
//!   construct a [`DataframeQueryTableProvider`] (does schema discovery + sets
//!   up the gRPC connection), and register it on the embedded
//!   [`SessionContext`] under the given name.
//! - `listRerunSegmentsNative`: enumerate segment ids for the dataset, used by
//!   the Spark driver to plan one input partition per segment.

use std::sync::{Arc, Once};

use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema, TimeUnit};
use datafusion::catalog::TableProvider;
use datafusion::prelude::SessionContext;
use jni::objects::{JByteArray, JClass, JString};
use jni::sys::{jlong, jobjectArray};
use jni::JNIEnv;
use prost::Message;

use re_datafusion::DataframeQueryTableProvider;
use re_dataframe::QueryExpression;
use re_log_types::EntryId;
use re_protos::cloud::v1alpha1::EntryFilter;
use re_redap_client::{ConnectionClient, ConnectionRegistry, ConnectionRegistryHandle, Credentials};
use re_types_core::TimelineName;

use crate::errors::{try_unwrap_or_throw, JniResult};
use crate::proto_gen::RerunTableOptions;
use crate::runtime;

/// Idempotent install of rustls's `ring` crypto provider. The rerun TLS stack
/// crashes at runtime if no default provider is installed; this used to be
/// done implicitly by `object_store` but rerun no longer pulls that in, so the
/// JNI bridge installs it explicitly on first use.
fn init_rustls_crypto() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // `install_default` returns Err when a provider has already been
        // installed by another path; we don't care which one wins.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn build_registry_handle(
    origin: &re_uri::Origin,
    token: &str,
) -> JniResult<ConnectionRegistryHandle> {
    let handle = ConnectionRegistry::new_with_stored_credentials();
    let credentials = if token.is_empty() {
        Credentials::Stored
    } else {
        let jwt = re_auth::Jwt::try_from(token.to_owned())?;
        Credentials::Token(jwt)
    };
    handle.set_credentials(origin, credentials);
    Ok(handle)
}

async fn resolve_entry_id(
    handle: &ConnectionRegistryHandle,
    origin: &re_uri::Origin,
    options: &RerunTableOptions,
) -> JniResult<EntryId> {
    if !options.dataset_id.is_empty() {
        let id: EntryId = options
            .dataset_id
            .parse()
            .map_err(|e: std::num::ParseIntError| -> Box<dyn std::error::Error + Send + Sync> {
                format!("invalid Rerun dataset_id {:?}: {}", options.dataset_id, e).into()
            })?;
        return Ok(id);
    }
    if options.dataset_name.is_empty() {
        return Err("RerunTableOptions: one of `dataset_id` or `dataset_name` must be set".into());
    }
    let mut client = handle.client(origin.clone()).await?;
    let entries = client
        .find_entries(EntryFilter::new().with_name(options.dataset_name.clone()))
        .await?;
    let entry = entries.into_iter().next().ok_or_else(
        || -> Box<dyn std::error::Error + Send + Sync> {
            format!("no Rerun entry found with name {:?}", options.dataset_name).into()
        },
    )?;
    Ok(entry.id)
}

fn build_query_expression(options: &RerunTableOptions) -> QueryExpression {
    let mut qe = QueryExpression::default();
    if !options.index.is_empty() {
        qe.filtered_index = Some(TimelineName::new(options.index.as_str()));
    }
    qe
}

async fn build_provider(
    options: RerunTableOptions,
) -> JniResult<Arc<dyn TableProvider>> {
    init_rustls_crypto();
    let origin: re_uri::Origin = options
        .url
        .parse()
        .map_err(|e: re_uri::Error| -> Box<dyn std::error::Error + Send + Sync> {
            format!("invalid Rerun url {:?}: {}", options.url, e).into()
        })?;
    let handle = build_registry_handle(&origin, options.token.as_str())?;
    let entry_id = resolve_entry_id(&handle, &origin, &options).await?;
    let query_expr = build_query_expression(&options);

    let provider = DataframeQueryTableProvider::<ConnectionClient>::new(
        origin,
        handle,
        entry_id,
        &query_expr,
        options.segments.as_slice(),
        None,
        None,
        None,
        Vec::new(),
    )
    .await?;
    Ok(Arc::new(provider))
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_registerRerunTableNative<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    name: JString<'local>,
    options_proto: JByteArray<'local>,
) {
    try_unwrap_or_throw(&mut env, (), |env| -> JniResult<()> {
        if handle == 0 {
            return Err("SessionContext handle is null".into());
        }
        // SAFETY: matches the existing `registerTableNative` pattern — handle
        // came from `createSessionContext` as `Box<SessionContext>` raw ptr.
        let ctx = unsafe { &*(handle as *const SessionContext) };
        let name: String = env.get_string(&name)?.into();
        let bytes: Vec<u8> = env.convert_byte_array(&options_proto)?;
        let options = RerunTableOptions::decode(bytes.as_slice())?;

        let provider = runtime().block_on(build_provider(options))?;
        runtime().block_on(register_with_widening_view(ctx, name.as_str(), provider))?;
        Ok(())
    })
}

/// Recursively compute the arrow_cast destination-type string for a column
/// whose Arrow type is not directly readable by Spark's `ArrowColumnVector`.
///
/// Spark 3.5's `ArrowColumnVector` has no accessor for unsigned ints, Time,
/// or Float16. We use DataFusion's built-in `arrow_cast(col, '<type>')`
/// (rather than SQL `CAST`) because it preserves nested structure — a
/// `List<UInt16>` becomes `List(Int32)` end-to-end, not just at the top
/// level. Returns `Some(target)` if any widening is needed, `None` if the
/// column passes through unchanged.
///
/// Coverage:
///   - scalars: UInt8/16/32/64, Float16, Time32/64
///   - List<...>, LargeList<...>, FixedSizeList<..., size> with a widenable
///     element type (handles the `item` field rejection at any nesting depth)
///
/// NOT covered in v1: Struct<...> / Map<...> with widenable children. The
/// JVM schema converter still rejects those with the original error.
fn arrow_cast_widening(dt: &DataType) -> Option<String> {
    match dt {
        DataType::UInt8 => Some("Int16".into()),
        DataType::UInt16 => Some("Int32".into()),
        DataType::UInt32 => Some("Int64".into()),
        // UInt64 widening is lossy for values ≥ 2^63 — documented limitation.
        DataType::UInt64 => Some("Int64".into()),
        DataType::Float16 => Some("Float32".into()),
        DataType::Time32(_) => Some("Int32".into()),
        DataType::Time64(_) => Some("Int64".into()),
        // Spark's ArrowColumnVector accepts only Timestamp(Microsecond, ...).
        // Other units cause `UNSUPPORTED_ARROWTYPE` at executor batch wrap.
        // Cast all timestamps to microsecond precision, preserving the
        // timezone string (None vs Some(tz)).
        DataType::Timestamp(unit, tz) => {
            if *unit == TimeUnit::Microsecond {
                None
            } else {
                let tz_str = match tz {
                    None => "None".to_string(),
                    Some(s) => format!("Some(\"{}\")", s.replace('\\', "\\\\").replace('"', "\\\"")),
                };
                Some(format!("Timestamp(Microsecond, {tz_str})"))
            }
        }
        DataType::List(field) => {
            arrow_cast_widening(field.data_type()).map(|t| format!("List({t})"))
        }
        DataType::LargeList(field) => {
            arrow_cast_widening(field.data_type()).map(|t| format!("LargeList({t})"))
        }
        DataType::FixedSizeList(field, size) => arrow_cast_widening(field.data_type())
            .map(|t| format!("FixedSizeList({t}, {size})")),
        _ => None,
    }
}

/// Register `provider` under `external_name`. If the provider's schema has any
/// fields that need a Spark-compatibility widen (see [`widen_sql_type`]), the
/// raw provider is stashed under a mangled name and `external_name` is
/// registered as a SQL view that casts the offending columns. v1 widens
/// top-level fields only — nested unsigned ints inside a Struct still surface
/// the original Arrow type and will fail in the JVM schema converter.
async fn register_with_widening_view(
    ctx: &SessionContext,
    external_name: &str,
    provider: Arc<dyn TableProvider>,
) -> JniResult<()> {
    let schema: Arc<ArrowSchema> = provider.schema();
    let needs_view = schema
        .fields()
        .iter()
        .any(|f| arrow_cast_widening(f.data_type()).is_some());

    if !needs_view {
        ctx.register_table(external_name, provider)?;
        return Ok(());
    }

    let raw_name = format!("__rerun_raw__{external_name}");
    ctx.register_table(raw_name.as_str(), provider)?;

    let select_list = schema
        .fields()
        .iter()
        .map(|f| {
            let name = f.name();
            // Identifier quoting: double quotes; escape any embedded "".
            let quoted = format!("\"{}\"", name.replace('"', "\"\""));
            match arrow_cast_widening(f.data_type()) {
                // arrow_cast preserves nested structure (List<UInt16> →
                // List(Int32)); SQL CAST would have to be List-of-scalar
                // only and produce a different operator graph.
                Some(target) => format!("arrow_cast({quoted}, '{target}') AS {quoted}"),
                None => quoted,
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT {select_list} FROM \"{}\"",
        raw_name.replace('"', "\"\"")
    );

    let df = ctx.sql(&sql).await?;
    ctx.register_table(external_name, df.into_view())?;
    Ok(())
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_listRerunSegmentsNative<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    options_proto: JByteArray<'local>,
) -> jobjectArray {
    try_unwrap_or_throw(
        &mut env,
        std::ptr::null_mut(),
        |env| -> JniResult<jobjectArray> {
            let bytes: Vec<u8> = env.convert_byte_array(&options_proto)?;
            let options = RerunTableOptions::decode(bytes.as_slice())?;

            init_rustls_crypto();
            let origin: re_uri::Origin = options.url.parse().map_err(
                |e: re_uri::Error| -> Box<dyn std::error::Error + Send + Sync> {
                    format!("invalid Rerun url {:?}: {}", options.url, e).into()
                },
            )?;
            let handle = build_registry_handle(&origin, options.token.as_str())?;

            let segments: Vec<String> = runtime().block_on(async {
                let entry_id = resolve_entry_id(&handle, &origin, &options).await?;
                let mut client = handle.client(origin.clone()).await?;
                let raw = client.get_dataset_segment_ids(entry_id).await?;
                Ok::<Vec<String>, Box<dyn std::error::Error + Send + Sync>>(
                    raw.into_iter().map(|s| s.into_inner()).collect(),
                )
            })?;

            let string_class = env.find_class("java/lang/String")?;
            let empty = env.new_string("")?;
            let arr = env.new_object_array(segments.len() as i32, &string_class, &empty)?;
            for (i, s) in segments.iter().enumerate() {
                let js = env.new_string(s)?;
                env.set_object_array_element(&arr, i as i32, js)?;
            }
            Ok(arr.into_raw())
        },
    )
}
