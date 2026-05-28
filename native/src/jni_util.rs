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

//! Small shared helpers for JNI call sites.

use jni::objects::JThrowable;
use jni::sys::jbyte;
use jni::JNIEnv;

/// Verbosity of the throwable representation produced when a JVM upcall
/// (UDF, TableProvider) throws. Mirrors the public Java `ExceptionVerbosity`
/// enum 1:1; the byte tag is what crosses the JNI boundary on
/// `registerScalarUdf` / `registerTableNative`. Byte 0 is reserved as a
/// "bogus" sentinel (rejected by `from_byte`) to make accidental zero-init
/// surface as a clear error rather than silently picking FULL.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum ExceptionVerbosity {
    /// Class name + message + Java stack trace.
    Full,
    /// Class name + message only (matches pre-#55 behaviour).
    Message,
    /// Class name only.
    None,
}

impl ExceptionVerbosity {
    /// Decode the byte tag the Java side passes through `register*` JNI calls.
    /// The mapping matches the proto enum: 1 = FULL, 2 = MESSAGE, 3 = NONE.
    /// Anything else (including 0 = UNSPECIFIED) returns an error so the
    /// caller can surface a clear "this byte is bogus" diagnostic instead of
    /// silently defaulting to FULL.
    pub(crate) fn from_byte(b: jbyte) -> Result<Self, String> {
        match b {
            1 => Ok(Self::Full),
            2 => Ok(Self::Message),
            3 => Ok(Self::None),
            _ => Err(format!("invalid ExceptionVerbosity byte: {b}")),
        }
    }
}

/// Best-effort: render a Java throwable as a string at the requested
/// verbosity. Anything that goes wrong while inspecting the throwable
/// collapses to a placeholder so we don't double-throw inside an error path.
///
/// `kind` and `name` are used to build the surfaced error message
/// (e.g., `kind="UDF" name="add_one"` -> `"Java UDF 'add_one' threw ..."`).
pub(crate) fn jthrowable_to_string(
    env: &mut JNIEnv,
    throwable: &JThrowable,
    kind: &str,
    name: &str,
    verbosity: ExceptionVerbosity,
) -> String {
    let class_name = jthrowable_class_name(env, throwable);

    if verbosity == ExceptionVerbosity::None {
        return format!("Java {} '{}' threw {}", kind, name, class_name);
    }

    let message = jthrowable_message(env, throwable);
    let header = match message {
        Some(m) => format!("Java {} '{}' threw {}: {}", kind, name, class_name, m),
        None => format!("Java {} '{}' threw {}", kind, name, class_name),
    };

    if verbosity == ExceptionVerbosity::Message {
        return header;
    }

    // Full verbosity: append the Java-formatted stack trace. We delegate to
    // `Throwable.printStackTrace(PrintWriter)` so the format matches every
    // standard Java logger (header line + "\tat fqn(File:line)" frames).
    match jthrowable_stack_trace(env, throwable) {
        Ok(trace) => format!("{}\n{}", header, trace.trim_end()),
        Err(_) => {
            // Trace rendering can leave a pending JNI exception (e.g. a
            // custom `Throwable.printStackTrace` overload that itself
            // throws, or OOM during StringWriter/PrintWriter allocation).
            // Clear it before returning -- a stale pending exception would
            // poison the next JNI call and cause try_unwrap_or_throw to
            // surface the secondary exception instead of the original
            // DataFusionError.
            env.exception_clear().ok();
            header
        }
    }
}

fn jthrowable_class_name(env: &mut JNIEnv, throwable: &JThrowable) -> String {
    let result = (|| -> jni::errors::Result<String> {
        let class = env.call_method(throwable, "getClass", "()Ljava/lang/Class;", &[])?;
        let class_obj = class.l()?;
        let n = env.call_method(&class_obj, "getName", "()Ljava/lang/String;", &[])?;
        let n_obj = n.l()?;
        let n_str: String = env.get_string(&n_obj.into())?.into();
        Ok(n_str)
    })();
    match result {
        Ok(s) => s,
        Err(_) => {
            env.exception_clear().ok();
            "<unknown exception class>".to_string()
        }
    }
}

fn jthrowable_message(env: &mut JNIEnv, throwable: &JThrowable) -> Option<String> {
    let result = (|| -> jni::errors::Result<Option<String>> {
        let msg = env.call_method(throwable, "getMessage", "()Ljava/lang/String;", &[])?;
        let msg_obj = msg.l()?;
        if msg_obj.is_null() {
            return Ok(None);
        }
        let s: String = env.get_string(&msg_obj.into())?.into();
        Ok(Some(s))
    })();
    match result {
        Ok(opt) => opt,
        Err(_) => {
            env.exception_clear().ok();
            None
        }
    }
}

fn jthrowable_stack_trace(env: &mut JNIEnv, throwable: &JThrowable) -> jni::errors::Result<String> {
    let sw = env.new_object("java/io/StringWriter", "()V", &[])?;
    let pw_args = [(&sw).into()];
    let pw = env.new_object("java/io/PrintWriter", "(Ljava/io/Writer;)V", &pw_args)?;
    let trace_args = [(&pw).into()];
    env.call_method(
        throwable,
        "printStackTrace",
        "(Ljava/io/PrintWriter;)V",
        &trace_args,
    )?;
    let s = env.call_method(&sw, "toString", "()Ljava/lang/String;", &[])?;
    let s_obj = s.l()?;
    let s_str: String = env.get_string(&s_obj.into())?.into();
    Ok(s_str)
}
