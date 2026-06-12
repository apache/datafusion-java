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

use std::any::Any;
use std::error::Error;
use std::panic::{catch_unwind, AssertUnwindSafe};

use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use jni::JNIEnv;

pub type JniResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// Run `f`, catching panics and translating its `Err` into the appropriate
/// Java exception. When the boxed error is a [`DataFusionError`], the variant
/// drives which Java exception class is thrown -- callers can `catch
/// (PlanException)` / `catch (ResourcesExhaustedException)` etc. without
/// scraping message strings. Anything else (including Rust panics) falls
/// through to the parent [`DataFusionException`] class.
pub fn try_unwrap_or_throw<T, F>(env: &mut JNIEnv, default: T, f: F) -> T
where
    F: FnOnce(&mut JNIEnv) -> JniResult<T>,
{
    match catch_unwind(AssertUnwindSafe(|| f(env))) {
        Ok(Ok(value)) => value,
        Ok(Err(boxed)) => {
            // Try to recover the typed DataFusionError so we can pick a
            // matching Java subclass. If the error came from elsewhere
            // (jni::errors::Error, prost::DecodeError, a stringly-typed
            // Err("...")?), it falls through to the parent class.
            match boxed.downcast::<DataFusionError>() {
                Ok(df_err) => {
                    // Walk the cause chain so a ResourcesExhausted / Plan /
                    // etc. wrapped in ArrowError::External (or any other
                    // upstream-mandated wrapper) is classified by its real
                    // root, not by the outer wrapper. Same shape as upstream's
                    // `DataFusionError::find_root()`. The thrown message
                    // preserves the outer error's full chain so wrapping
                    // context isn't lost; only the *class* picks the inner
                    // variant.
                    let class = classify(df_err.find_root());
                    throw(env, class, &df_err.to_string());
                }
                Err(other) => {
                    throw(env, PARENT, &other.to_string());
                }
            }
            default
        }
        Err(panic) => {
            // Rust panics are upstream-bug-shaped, not caller-actionable.
            // Surface as the parent class with a `panic: ` prefix so the
            // caller can grep without inspecting a typed subclass.
            throw(env, PARENT, &format!("panic: {}", panic_message(&panic)));
            default
        }
    }
}

const PARENT: &str = "org/apache/datafusion/DataFusionException";

/// Map a [`DataFusionError`] variant onto the Java exception class to throw.
/// Multiple Rust variants fold into a single Java class when they're the same
/// kind of problem from the caller's standpoint -- the proposed mapping is
/// the public Java contract; the underlying variant set is not.
///
/// Callers should pass the result of [`DataFusionError::find_root`] so a
/// `ResourcesExhausted` / `Plan` / etc. buried inside a wrapping `ArrowError`
/// or `Context(...)` is classified by its real root. Variants without a
/// clean caller-facing category fall through to the parent class.
fn classify(err: &DataFusionError) -> &'static str {
    match err {
        DataFusionError::Plan(_)
        | DataFusionError::SQL(_, _)
        | DataFusionError::SchemaError(_, _) => "org/apache/datafusion/PlanException",
        DataFusionError::Execution(_)
        | DataFusionError::ExecutionJoin(_)
        | DataFusionError::External(_)
        | DataFusionError::Ffi(_) => "org/apache/datafusion/ExecutionException",
        DataFusionError::ResourcesExhausted(_) => {
            "org/apache/datafusion/ResourcesExhaustedException"
        }
        DataFusionError::IoError(_)
        | DataFusionError::ObjectStore(_)
        | DataFusionError::ParquetError(_) => "org/apache/datafusion/IoException",
        // The AvroError variant only exists when DataFusion is built with its
        // `avro` feature, forwarded by this crate's own `avro` feature.
        #[cfg(feature = "avro")]
        DataFusionError::AvroError(_) => "org/apache/datafusion/IoException",
        // ArrowError is a 21-variant grab bag -- only some of those variants
        // are actually IO-shaped. DivideByZero / ArithmeticOverflow / Compute
        // / Cast / InvalidArgument / Memory etc. are execution-time failures
        // produced by a normal query (e.g. SELECT 1/0), not IO. Routing them
        // through IoException would hide ordinary execution errors from
        // callers catching ExecutionException.
        DataFusionError::ArrowError(arrow_err, _) => classify_arrow(arrow_err),
        DataFusionError::NotImplemented(_) => "org/apache/datafusion/NotImplementedException",
        DataFusionError::Configuration(_) => "org/apache/datafusion/ConfigurationException",
        // Variants with no clean caller-facing category -- Internal (upstream
        // bug-shaped), Substrait, Collection (multi-error), and any new
        // variants a future DataFusion bump introduces -- fall through to
        // the parent. The catch-all is deliberate: "I don't know what to do
        // with this" surfaces as the parent class rather than a wrong typed
        // subclass. Wrapper variants like Context / Diagnostic / Shared also
        // land here when find_root() can't dig past them, which is rare
        // because find_root walks `source()`.
        _ => PARENT,
    }
}

/// Map an [`ArrowError`] variant onto the Java exception class to throw.
/// Only the genuinely IO-shaped variants (`IoError`, `IpcError`) land on
/// `IoException`; everything else is execution-time and routes through
/// `ExecutionException`. Schema/parse-shaped variants route through
/// `PlanException` so a malformed IPC schema or a parse error surfaces as a
/// query problem rather than an execution failure.
///
/// Variants without a clean caller-facing category (`CDataInterface`, the
/// various overflow/index-overflow markers) fall through to the parent.
fn classify_arrow(err: &ArrowError) -> &'static str {
    match err {
        ArrowError::IoError(_, _) | ArrowError::IpcError(_) => "org/apache/datafusion/IoException",
        ArrowError::SchemaError(_) | ArrowError::ParseError(_) => {
            "org/apache/datafusion/PlanException"
        }
        ArrowError::DivideByZero
        | ArrowError::ArithmeticOverflow(_)
        | ArrowError::ComputeError(_)
        | ArrowError::CastError(_)
        | ArrowError::InvalidArgumentError(_)
        | ArrowError::MemoryError(_)
        | ArrowError::CsvError(_)
        | ArrowError::JsonError(_)
        | ArrowError::AvroError(_)
        | ArrowError::ParquetError(_)
        | ArrowError::ExternalError(_) => "org/apache/datafusion/ExecutionException",
        ArrowError::NotYetImplemented(_) => "org/apache/datafusion/NotImplementedException",
        // CDataInterface, DictionaryKeyOverflowError, RunEndIndexOverflowError,
        // OffsetOverflowError, and any new variants in future Arrow bumps fall
        // through to the parent -- "I don't know what to do with this" stays
        // at the parent rather than getting routed to the wrong subclass.
        _ => PARENT,
    }
}

fn throw(env: &mut JNIEnv, class: &str, message: &str) {
    if env.exception_check().unwrap_or(false) {
        return;
    }
    let _ = env.throw_new(class, message);
}

/// Best-effort extraction of a panic payload's message. `catch_unwind` hands
/// back a `Box<dyn Any>`; the payload is a `String` or `&str` for ordinary
/// `panic!`/`unwrap` sites, anything else is opaque.
pub fn panic_message(panic: &Box<dyn Any + Send>) -> String {
    if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = panic.downcast_ref::<&str>() {
        (*s).to_string()
    } else {
        "rust panic with non-string payload".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::error::ArrowError;

    #[test]
    fn classify_unwrapped_variants_picks_typed_class() {
        // Sanity check: the simple unwrapped cases land on the right classes.
        assert_eq!(
            classify(&DataFusionError::Plan("x".into())),
            "org/apache/datafusion/PlanException"
        );
        assert_eq!(
            classify(&DataFusionError::ResourcesExhausted("x".into())),
            "org/apache/datafusion/ResourcesExhaustedException"
        );
        assert_eq!(
            classify(&DataFusionError::Configuration("x".into())),
            "org/apache/datafusion/ConfigurationException"
        );
    }

    #[test]
    fn classify_unrecognised_variant_falls_through_to_parent() {
        // `Internal` has no clean caller-facing category; must surface as
        // the parent class rather than getting routed to a wrong subclass.
        assert_eq!(classify(&DataFusionError::Internal("x".into())), PARENT);
    }

    #[test]
    fn classify_via_find_root_unwraps_nested_resources_exhausted() {
        // Codex regression: a DataFusionError::ResourcesExhausted buried
        // inside an ArrowError::ExternalError(Box<DataFusionError::Context>)
        // wrapper would otherwise land on the outer ArrowError arm and be
        // classified as IoException -- the wrong typed exception. Confirm
        // that calling classify on `find_root` recovers the inner variant
        // (the same shape upstream's own find_root() doctest pins).
        let inner = DataFusionError::ResourcesExhausted("memory pool full".into());
        let contexted = DataFusionError::Context("aggregate stream".into(), Box::new(inner));
        let arrow_wrapped = ArrowError::ExternalError(Box::new(contexted));
        let outer = DataFusionError::ArrowError(Box::new(arrow_wrapped), None);

        // Without find_root, the outer would route to IoException; with it,
        // the real root (ResourcesExhausted) wins.
        assert_eq!(
            classify(outer.find_root()),
            "org/apache/datafusion/ResourcesExhaustedException"
        );
    }

    #[test]
    fn classify_via_find_root_unwraps_plan_through_context() {
        // A Plan error wrapped only in DataFusionError::Context still has to
        // reach PlanException, not the parent class.
        let inner = DataFusionError::Plan("table 't' not found".into());
        let contexted = DataFusionError::Context("logical planning".into(), Box::new(inner));

        assert_eq!(
            classify(contexted.find_root()),
            "org/apache/datafusion/PlanException"
        );
    }

    #[test]
    fn arrow_divide_by_zero_routes_to_execution_not_io() {
        // Codex regression: DataFusionError::ArrowError(ArrowError::DivideByZero)
        // is the actual shape produced by `SELECT 1/0`. The 21-variant
        // ArrowError grab bag means routing the whole ArrowError arm to
        // IoException would put ordinary divide-by-zero / overflow / cast
        // errors on the wrong typed exception. classify_arrow inspects the
        // inner variant.
        let outer = DataFusionError::ArrowError(Box::new(ArrowError::DivideByZero), None);
        assert_eq!(
            classify(outer.find_root()),
            "org/apache/datafusion/ExecutionException"
        );

        let outer = DataFusionError::ArrowError(
            Box::new(ArrowError::ArithmeticOverflow("i32 overflow".into())),
            None,
        );
        assert_eq!(
            classify(outer.find_root()),
            "org/apache/datafusion/ExecutionException"
        );

        let outer = DataFusionError::ArrowError(
            Box::new(ArrowError::CastError("Int32 -> Date32".into())),
            None,
        );
        assert_eq!(
            classify(outer.find_root()),
            "org/apache/datafusion/ExecutionException"
        );
    }

    #[test]
    fn arrow_io_variants_still_route_to_io() {
        // Genuinely IO-shaped ArrowError variants stay on IoException.
        let io = std::io::Error::other("disk full");
        let outer = DataFusionError::ArrowError(
            Box::new(ArrowError::IoError("write failed".into(), io)),
            None,
        );
        assert_eq!(
            classify(outer.find_root()),
            "org/apache/datafusion/IoException"
        );

        let outer =
            DataFusionError::ArrowError(Box::new(ArrowError::IpcError("bad framing".into())), None);
        assert_eq!(
            classify(outer.find_root()),
            "org/apache/datafusion/IoException"
        );
    }

    #[test]
    fn arrow_schema_and_parse_errors_route_to_plan() {
        // SchemaError and ParseError are query-shaped problems (you wrote a
        // bad query / supplied a bad schema), not execution failures.
        let outer = DataFusionError::ArrowError(
            Box::new(ArrowError::SchemaError("column 'foo' not found".into())),
            None,
        );
        assert_eq!(
            classify(outer.find_root()),
            "org/apache/datafusion/PlanException"
        );

        let outer = DataFusionError::ArrowError(
            Box::new(ArrowError::ParseError("expected int".into())),
            None,
        );
        assert_eq!(
            classify(outer.find_root()),
            "org/apache/datafusion/PlanException"
        );
    }
}
