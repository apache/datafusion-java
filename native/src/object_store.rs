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

//! Apply [`ObjectStoreRegistration`] entries from the session-options proto to a
//! [`SessionContext`]'s [`RuntimeEnv`].
//!
//! Each backend (`s3` / `gcs` / `http`) is gated behind its own Cargo feature;
//! the corresponding arm of the proto `oneof` returns a clear error if the
//! feature was not enabled at build time. The default build of this crate
//! enables all three so Java callers don't have to think about features.

use std::sync::Arc;

use datafusion::prelude::SessionContext;
use url::Url;

use crate::errors::JniResult;
use crate::proto_gen::object_store_registration::Backend;
use crate::proto_gen::ObjectStoreRegistration;

#[cfg(feature = "object-store-gcp")]
use crate::proto_gen::GcsOptions;
#[cfg(feature = "object-store-http")]
use crate::proto_gen::HttpOptions;
#[cfg(feature = "object-store-aws")]
use crate::proto_gen::S3Options;

/// Apply every registration in `regs`, in order, to the context's `RuntimeEnv`.
/// If two registrations resolve to the same URL, the later one wins (matching
/// upstream `RuntimeEnv::register_object_store`).
pub(crate) fn apply_registrations(
    ctx: &SessionContext,
    regs: &[ObjectStoreRegistration],
) -> JniResult<()> {
    for reg in regs {
        let backend = reg
            .backend
            .as_ref()
            .ok_or("ObjectStoreRegistration.backend is required")?;
        let (url, store) = build_store(reg.url.as_deref(), backend)?;
        ctx.runtime_env().register_object_store(&url, store);
    }
    Ok(())
}

#[allow(unused_variables)] // `url_override` is unused on builds with no features
fn build_store(
    url_override: Option<&str>,
    backend: &Backend,
) -> JniResult<(Url, Arc<dyn object_store::ObjectStore>)> {
    match backend {
        #[cfg(feature = "object-store-aws")]
        Backend::S3(opts) => build_s3(url_override, opts),
        #[cfg(not(feature = "object-store-aws"))]
        Backend::S3(_) => Err(
            "object-store-aws Cargo feature is not enabled in this build of datafusion-jni".into(),
        ),

        #[cfg(feature = "object-store-gcp")]
        Backend::Gcs(opts) => build_gcs(url_override, opts),
        #[cfg(not(feature = "object-store-gcp"))]
        Backend::Gcs(_) => Err(
            "object-store-gcp Cargo feature is not enabled in this build of datafusion-jni".into(),
        ),

        #[cfg(feature = "object-store-http")]
        Backend::Http(opts) => build_http(url_override, opts),
        #[cfg(not(feature = "object-store-http"))]
        Backend::Http(_) => Err(
            "object-store-http Cargo feature is not enabled in this build of datafusion-jni".into(),
        ),
    }
}

#[cfg(feature = "object-store-aws")]
fn build_s3(
    url_override: Option<&str>,
    opts: &S3Options,
) -> JniResult<(Url, Arc<dyn object_store::ObjectStore>)> {
    use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};

    if opts.bucket.is_empty() {
        return Err("S3Options.bucket is required".into());
    }

    // Seed from the standard AWS env (AWS_ACCESS_KEY_ID, AWS_DEFAULT_REGION,
    // AWS_WEB_IDENTITY_TOKEN_FILE, ECS task creds, etc.) before applying
    // explicit Java overrides. Without this, callers running on EC2/ECS/EKS
    // who omit Java-side credentials and expect the SDK default-credential
    // chain would get an empty builder and fail at request time.
    //
    // Walk env explicitly (instead of AmazonS3Builder::from_env()) so we can
    // skip endpoint keys when the Java caller set `endpoint(...)`.
    // AmazonS3Builder::build() picks `s3_endpoint` over `endpoint`
    // (object_store 0.13 aws/builder.rs:1209), so a stray
    // AWS_ENDPOINT_URL_S3 in the JVM env would otherwise silently override
    // the explicit Java endpoint -- breaking MinIO/R2/etc. registrations
    // run out of a process with global AWS env vars.
    let java_has_endpoint = opts.endpoint.is_some();
    let mut b = AmazonS3Builder::default();
    for (os_key, os_value) in std::env::vars_os() {
        let (Some(key), Some(value)) = (os_key.to_str(), os_value.to_str()) else {
            continue;
        };
        if !key.starts_with("AWS_") {
            continue;
        }
        if java_has_endpoint
            && (key == "AWS_ENDPOINT" || key == "AWS_ENDPOINT_URL" || key == "AWS_ENDPOINT_URL_S3")
        {
            continue;
        }
        // Skip AWS_BUCKET / AWS_BUCKET_NAME: the Java caller's bucket is the
        // identity of this registration (it's also the host portion of the
        // registry-key URL we return). An env-supplied bucket would overwrite
        // it here while the URL still says `s3://<opts.bucket>`, so a query
        // for the Java bucket would be served by the env bucket. Apply the
        // Java bucket explicitly *after* the loop so it can't be clobbered.
        if key == "AWS_BUCKET" || key == "AWS_BUCKET_NAME" {
            continue;
        }
        if let Ok(config_key) = key.to_ascii_lowercase().parse() {
            b = b.with_config(config_key, value);
        }
    }
    b = b.with_bucket_name(&opts.bucket);
    if let Some(ref v) = opts.region {
        b = b.with_region(v);
    }
    if let Some(ref v) = opts.endpoint {
        b = b.with_endpoint(v);
    }
    if let Some(ref v) = opts.access_key_id {
        b = b.with_access_key_id(v);
    }
    if let Some(ref v) = opts.secret_access_key {
        b = b.with_secret_access_key(v);
    }
    if let Some(ref v) = opts.session_token {
        b = b.with_token(v);
    }
    if let Some(v) = opts.allow_http {
        b = b.with_allow_http(v);
    }
    if let Some(v) = opts.skip_signature {
        b = b.with_skip_signature(v);
    }
    if let Some(v) = opts.imdsv1_fallback {
        // AmazonS3Builder::with_imdsv1_fallback() only sets to `true` (no
        // boolean arg). To honor explicit `imdsv1Fallback(false)` and
        // override an env-seeded `AWS_IMDSV1_FALLBACK=true`, write through
        // with_config(...) which always sets the field, both directions.
        b = b.with_config(AmazonS3ConfigKey::ImdsV1Fallback, v.to_string());
    }

    let store = b.build()?;
    let url = parse_url(url_override, format!("s3://{}", opts.bucket))?;
    Ok((url, Arc::new(store)))
}

#[cfg(feature = "object-store-gcp")]
fn build_gcs(
    url_override: Option<&str>,
    opts: &GcsOptions,
) -> JniResult<(Url, Arc<dyn object_store::ObjectStore>)> {
    use object_store::gcp::GoogleCloudStorageBuilder;

    if opts.bucket.is_empty() {
        return Err("GcsOptions.bucket is required".into());
    }
    if opts.service_account_key.is_some() && opts.service_account_path.is_some() {
        return Err(
            "GcsOptions: service_account_key and service_account_path are mutually exclusive"
                .into(),
        );
    }

    // Seed from the standard GCS env (GOOGLE_BUCKET, GOOGLE_SERVICE_ACCOUNT,
    // etc.) before applying explicit Java overrides.
    //
    // Walk env explicitly (instead of GoogleCloudStorageBuilder::from_env())
    // so we can skip credential keys that conflict with Java-supplied
    // credentials. GoogleCloudStorageBuilder::build() rejects the combo of
    // service_account_path AND service_account_key with
    // ServiceAccountPathAndKeyProvided (object_store 0.13 gcp/builder.rs:525)
    // -- so if env has GOOGLE_SERVICE_ACCOUNT_KEY and Java sets
    // serviceAccountPath(...), the build would fail. Skip env credential
    // keys when Java provides any of the three credential fields.
    let java_has_credential = opts.service_account_key.is_some()
        || opts.service_account_path.is_some()
        || opts.application_credentials.is_some();
    let mut b = GoogleCloudStorageBuilder::default();
    for (os_key, os_value) in std::env::vars_os() {
        let (Some(key), Some(value)) = (os_key.to_str(), os_value.to_str()) else {
            continue;
        };
        if !key.starts_with("GOOGLE_") {
            continue;
        }
        if java_has_credential
            && (key == "GOOGLE_SERVICE_ACCOUNT"
                || key == "GOOGLE_SERVICE_ACCOUNT_PATH"
                || key == "GOOGLE_SERVICE_ACCOUNT_KEY"
                || key == "GOOGLE_APPLICATION_CREDENTIALS")
        {
            continue;
        }
        // Skip GOOGLE_BUCKET / GOOGLE_BUCKET_NAME: the Java caller's bucket is
        // the identity of this registration (matches the registry-key URL).
        // Apply the Java bucket explicitly *after* the loop so an env-supplied
        // bucket can't silently route the registered URL to a different
        // backend bucket.
        if key == "GOOGLE_BUCKET" || key == "GOOGLE_BUCKET_NAME" {
            continue;
        }
        if let Ok(config_key) = key.to_ascii_lowercase().parse() {
            b = b.with_config(config_key, value);
        }
    }
    b = b.with_bucket_name(&opts.bucket);
    if let Some(ref v) = opts.service_account_key {
        b = b.with_service_account_key(v);
    }
    if let Some(ref v) = opts.service_account_path {
        b = b.with_service_account_path(v);
    }
    if let Some(ref v) = opts.application_credentials {
        b = b.with_application_credentials(v);
    }

    let store = b.build()?;
    let url = parse_url(url_override, format!("gs://{}", opts.bucket))?;
    Ok((url, Arc::new(store)))
}

#[cfg(feature = "object-store-http")]
fn build_http(
    url_override: Option<&str>,
    opts: &HttpOptions,
) -> JniResult<(Url, Arc<dyn object_store::ObjectStore>)> {
    use object_store::http::HttpBuilder;

    let listing = url_override.ok_or(
        "HttpOptions: ObjectStoreRegistration.url is required for the HTTP backend (no scheme-default)",
    )?;
    let listing_url =
        Url::parse(listing).map_err(|e| format!("invalid HTTP URL {listing:?}: {e}"))?;

    // DataFusion's DefaultObjectStoreRegistry::get_url_key strips paths from
    // the registry key (it keeps only scheme + host:port), so a registration
    // base like `https://example.com/data/` ends up under key
    // `https://example.com`. If we hand HttpBuilder the same pathful URL,
    // HttpStore prepends `/data/` to every object path on top of the path
    // DataFusion already supplied -- a request for
    // `https://example.com/data/file.parquet` becomes
    // `https://example.com/data/data/file.parquet`. Strip the URL to
    // scheme + authority for both the store base and the registry key so
    // they match upstream's lookup semantics; the SQL-side URL still carries
    // the full path, and HttpStore appends it once.
    if !listing_url.path().is_empty() && listing_url.path() != "/" {
        return Err(format!(
            "HttpOptions: listing URL must be a host root (no path component); \
             got {listing:?}. DataFusion's object-store registry keys ignore the \
             path portion of a URL, so a pathful base would cause every object \
             read to double-prepend the path. Register one HTTP store per \
             scheme+host and let SQL paths carry the rest of the URL."
        )
        .into());
    }

    let mut b = HttpBuilder::new().with_url(listing);
    if let Some(v) = opts.allow_http {
        // allow_http lives on ClientOptions in object_store 0.13, not directly
        // on HttpBuilder; route through with_client_options.
        b = b.with_client_options(object_store::ClientOptions::new().with_allow_http(v));
    }

    let store = b.build()?;
    Ok((listing_url, Arc::new(store)))
}

fn parse_url(url_override: Option<&str>, default: String) -> JniResult<Url> {
    let s = url_override.unwrap_or(&default);
    Url::parse(s).map_err(|e| format!("invalid object store URL {s:?}: {e}").into())
}
