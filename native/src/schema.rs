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

use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::ipc::reader::StreamReader;
use jni::objects::JByteArray;
use jni::JNIEnv;

use crate::errors::JniResult;

/// Decode an optional Arrow-IPC schema byte array passed in from Java.
/// Returns `None` if the byte-array reference is null.
pub(crate) fn decode_optional_schema(
    env: &mut JNIEnv,
    bytes: JByteArray,
) -> JniResult<Option<Schema>> {
    if bytes.is_null() {
        return Ok(None);
    }
    let buf: Vec<u8> = env.convert_byte_array(&bytes)?;
    let reader = StreamReader::try_new(std::io::Cursor::new(buf), None)?;
    Ok(Some((*reader.schema()).clone()))
}
