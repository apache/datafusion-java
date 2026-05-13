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

pub fn try_unwrap_or_throw<T, F>(env: &mut JNIEnv, default: T, f: F) -> T
where
    F: FnOnce(&mut JNIEnv) -> JniResult<T>,
{
    match catch_unwind(AssertUnwindSafe(|| f(env))) {
        Ok(Ok(value)) => value,
        Ok(Err(err)) => {
            throw_runtime_exception(env, &err.to_string());
            default
        }
        Err(panic) => {
            throw_runtime_exception(env, &panic_message(&panic));
            default
        }
    }
}

fn throw_runtime_exception(env: &mut JNIEnv, message: &str) {
    if env.exception_check().unwrap_or(false) {
        return;
    }
    let _ = env.throw_new("java/lang/RuntimeException", message);
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
