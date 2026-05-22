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
use jni::JNIEnv;

/// Best-effort: extract class name and `getMessage()` from a Java throwable.
/// Anything that goes wrong collapses to a generic message so we don't
/// double-throw inside an error path.
///
/// `kind` and `name` are used to build the surfaced error message
/// (e.g., `kind="UDF" name="add_one"` -> `"Java UDF 'add_one' threw ..."`).
pub(crate) fn jthrowable_to_string(
    env: &mut JNIEnv,
    throwable: &JThrowable,
    kind: &str,
    name: &str,
) -> String {
    let class_name_result = (|| -> jni::errors::Result<String> {
        let class = env.call_method(throwable, "getClass", "()Ljava/lang/Class;", &[])?;
        let class_obj = class.l()?;
        let n = env.call_method(&class_obj, "getName", "()Ljava/lang/String;", &[])?;
        let n_obj = n.l()?;
        let n_str: String = env.get_string(&n_obj.into())?.into();
        Ok(n_str)
    })();
    let class_name = match class_name_result {
        Ok(s) => s,
        Err(_) => {
            env.exception_clear().ok();
            "<unknown exception class>".to_string()
        }
    };

    let message_result = (|| -> jni::errors::Result<String> {
        let msg = env.call_method(throwable, "getMessage", "()Ljava/lang/String;", &[])?;
        let msg_obj = msg.l()?;
        if msg_obj.is_null() {
            return Ok("<no message>".to_string());
        }
        let s: String = env.get_string(&msg_obj.into())?.into();
        Ok(s)
    })();
    let message = match message_result {
        Ok(s) => s,
        Err(_) => {
            env.exception_clear().ok();
            "<no message>".to_string()
        }
    };

    format!("Java {} '{}' threw {}: {}", kind, name, class_name, message)
}
