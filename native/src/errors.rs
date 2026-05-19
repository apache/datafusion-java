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

use jni::JNIEnv;

pub type JniResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// Error message used to round-trip a query-cancellation signal from a JNI
/// handler to the Java side. `try_unwrap_or_throw` matches on this string and
/// throws a `java.util.concurrent.CancellationException` instead of the default
/// `RuntimeException`. Keep this stable across the codebase so the cancellation
/// path stays grep-friendly.
pub const CANCELLED_MESSAGE: &str = "datafusion-java: query cancelled";

pub fn try_unwrap_or_throw<T, F>(env: &mut JNIEnv, default: T, f: F) -> T
where
    F: FnOnce(&mut JNIEnv) -> JniResult<T>,
{
    match catch_unwind(AssertUnwindSafe(|| f(env))) {
        Ok(Ok(value)) => value,
        Ok(Err(err)) => {
            throw_for_message(env, &err.to_string());
            default
        }
        Err(panic) => {
            throw_for_message(env, &panic_message(&panic));
            default
        }
    }
}

fn throw_for_message(env: &mut JNIEnv, message: &str) {
    if env.exception_check().unwrap_or(false) {
        return;
    }
    let class = if message.contains(CANCELLED_MESSAGE) {
        "java/util/concurrent/CancellationException"
    } else {
        "java/lang/RuntimeException"
    };
    let _ = env.throw_new(class, message);
}

fn panic_message(panic: &Box<dyn Any + Send>) -> String {
    if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = panic.downcast_ref::<&str>() {
        (*s).to_string()
    } else {
        "rust panic with non-string payload".to_string()
    }
}
