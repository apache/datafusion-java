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

//! Java-backed scalar UDF support.

use std::any::Any;
use std::fmt;

use datafusion::arrow::datatypes::DataType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use jni::objects::{GlobalRef, JStaticMethodID};

pub(crate) struct JavaScalarUdf {
    pub(crate) name: String,
    pub(crate) signature: Signature,
    pub(crate) return_type: DataType,
    /// Global ref to the user's `org.apache.datafusion.ScalarUdf` instance.
    /// Read in Task 6 (invoke_with_args).
    #[allow(dead_code)]
    pub(crate) udf_global_ref: GlobalRef,
    /// Global ref to the `org.apache.datafusion.internal.JniBridge` class.
    /// Read in Task 6 (invoke_with_args).
    #[allow(dead_code)]
    pub(crate) bridge_class: GlobalRef,
    /// Method ID for `JniBridge.invokeScalarUdf`.
    /// Read in Task 6 (invoke_with_args).
    #[allow(dead_code)]
    pub(crate) invoke_method: JStaticMethodID,
}

// SAFETY: JStaticMethodID is a JNI handle that's safe to share because the
// class it points to is held alive by `bridge_class`. We never mutate
// `invoke_method` after construction; DataFusion requires `Send + Sync` on
// `ScalarUDFImpl`.
unsafe impl Send for JavaScalarUdf {}
unsafe impl Sync for JavaScalarUdf {}

impl fmt::Debug for JavaScalarUdf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JavaScalarUdf")
            .field("name", &self.name)
            .field("return_type", &self.return_type)
            .finish()
    }
}

impl PartialEq for JavaScalarUdf {
    fn eq(&self, other: &Self) -> bool {
        // Two Java UDFs are equal iff they wrap the same registered name.
        self.name == other.name
    }
}

impl Eq for JavaScalarUdf {}

impl std::hash::Hash for JavaScalarUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ScalarUDFImpl for JavaScalarUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        Err(DataFusionError::NotImplemented(format!(
            "JavaScalarUdf::invoke_with_args not yet implemented for '{}'",
            self.name
        )))
    }
}

pub(crate) fn volatility_from_byte(byte: u8) -> datafusion::error::Result<Volatility> {
    match byte {
        0 => Ok(Volatility::Immutable),
        1 => Ok(Volatility::Stable),
        2 => Ok(Volatility::Volatile),
        other => Err(DataFusionError::Execution(format!(
            "unknown volatility byte: {}",
            other
        ))),
    }
}
