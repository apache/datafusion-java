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

use datafusion::arrow::array::{make_array, Array, ArrayRef, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::arrow::ffi::{from_ffi, to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::common::ScalarValue;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use jni::objects::{GlobalRef, JStaticMethodID};
use jni::signature::{Primitive, ReturnType};
use jni::sys::{jbyte, jlong, jvalue};

pub(crate) struct JavaScalarUdf {
    pub(crate) name: String,
    pub(crate) signature: Signature,
    /// The full return Field as the Java caller declared it. Carries the data type plus
    /// nullability and any metadata; reused as both `return_type()` and the result of
    /// `return_field_from_args()` so callers see the user's declaration verbatim.
    pub(crate) return_field: FieldRef,
    /// Global ref to the user's `org.apache.datafusion.ScalarFunction` instance.
    pub(crate) udf_global_ref: GlobalRef,
    /// Global ref to the `org.apache.datafusion.internal.JniBridge` class.
    pub(crate) bridge_class: GlobalRef,
    /// Method ID for `JniBridge.invokeScalarUdf`.
    pub(crate) invoke_method: JStaticMethodID,
    /// Verbosity to apply when this UDF's `evaluate` throws. Snapshotted from
    /// the registering `SessionContext` -- locked at registration time, like
    /// the rest of this struct.
    pub(crate) verbosity: crate::jni_util::ExceptionVerbosity,
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
            .field("return_field", &self.return_field)
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
        Ok(self.return_field.data_type().clone())
    }

    fn return_field_from_args(
        &self,
        _args: ReturnFieldArgs,
    ) -> datafusion::error::Result<FieldRef> {
        // The default impl wraps return_type() in a fresh Field that's always nullable and
        // carries no metadata. We hold the user's declared Field verbatim, so return it -- this
        // preserves the declared nullability and any metadata they attached.
        Ok(self.return_field.clone())
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let number_rows = args.number_rows;

        let signature_types: &[DataType] = match &self.signature.type_signature {
            TypeSignature::Exact(types) => types,
            _ => {
                return Err(DataFusionError::Internal(
                    "JavaScalarUdf signature is not Exact; only Signature::exact is supported"
                        .to_string(),
                ))
            }
        };

        if args.args.len() != signature_types.len() {
            return Err(DataFusionError::Internal(format!(
                "Java UDF '{}' called with {} args; signature declares {}",
                self.name,
                args.args.len(),
                signature_types.len()
            )));
        }

        // 1. Partition args by kind. ColumnarValue::Scalar stays as a length-1 array so the Java
        //    side observes it as a Scalar; ColumnarValue::Array passes through at full length.
        let mut array_arrays: Vec<ArrayRef> = Vec::new();
        let mut array_fields: Vec<Field> = Vec::new();
        let mut scalar_arrays: Vec<ArrayRef> = Vec::new();
        let mut scalar_fields: Vec<Field> = Vec::new();
        let mut arg_kinds: Vec<u8> = Vec::with_capacity(args.args.len());

        for (i, cv) in args.args.iter().enumerate() {
            let ty = signature_types[i].clone();
            match cv {
                ColumnarValue::Array(a) => {
                    array_fields.push(Field::new(format!("arg{}", array_arrays.len()), ty, true));
                    array_arrays.push(a.clone());
                    arg_kinds.push(0);
                }
                ColumnarValue::Scalar(s) => {
                    let arr = s.to_array_of_size(1)?;
                    scalar_fields.push(Field::new(format!("arg{}", scalar_arrays.len()), ty, true));
                    scalar_arrays.push(arr);
                    arg_kinds.push(1);
                }
            }
        }

        // 2. Build the two struct arrays. Empty field+array vectors with the appropriate length
        //    cover nullary and all-one-kind cases.
        let array_struct = StructArray::try_new_with_length(
            Fields::from(array_fields),
            array_arrays,
            None,
            number_rows,
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        let scalar_struct =
            StructArray::try_new_with_length(Fields::from(scalar_fields), scalar_arrays, None, 1)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let (array_ffi_arr, array_ffi_sch) = to_ffi(&array_struct.into_data())
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        let (scalar_ffi_arr, scalar_ffi_sch) = to_ffi(&scalar_struct.into_data())
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        // 3. Pre-allocate empty FFI structs for the result.
        let result_ffi_arr = FFI_ArrowArray::empty();
        let result_ffi_sch = FFI_ArrowSchema::empty();

        // 4. Box for stable addresses across the JNI call.
        let mut array_arr_box = Box::new(array_ffi_arr);
        let mut array_sch_box = Box::new(array_ffi_sch);
        let mut scalar_arr_box = Box::new(scalar_ffi_arr);
        let mut scalar_sch_box = Box::new(scalar_ffi_sch);
        let mut result_arr_box = Box::new(result_ffi_arr);
        let mut result_sch_box = Box::new(result_ffi_sch);

        let array_arr_addr = array_arr_box.as_mut() as *mut _ as jlong;
        let array_sch_addr = array_sch_box.as_mut() as *mut _ as jlong;
        let scalar_arr_addr = scalar_arr_box.as_mut() as *mut _ as jlong;
        let scalar_sch_addr = scalar_sch_box.as_mut() as *mut _ as jlong;
        let result_arr_addr = result_arr_box.as_mut() as *mut _ as jlong;
        let result_sch_addr = result_sch_box.as_mut() as *mut _ as jlong;

        // 5. Attach JNI to current thread.
        let mut env = crate::jvm()
            .attach_current_thread()
            .map_err(|e| DataFusionError::Execution(format!("JNI attach failed: {}", e)))?;

        // 6. Build the byte[] for argKinds inside the JVM heap. JNI local; freed when env drops.
        let arg_kinds_array = env.byte_array_from_slice(&arg_kinds).map_err(|e| {
            DataFusionError::Execution(format!("byte_array_from_slice failed: {}", e))
        })?;

        let expected_rows = i32::try_from(number_rows).map_err(|_| {
            DataFusionError::Execution(format!(
                "batch row count {} exceeds i32::MAX; UDFs cannot handle batches larger than 2^31 - 1 rows",
                number_rows
            ))
        })?;

        let udf_jobject = self.udf_global_ref.as_obj();
        // SAFETY: udf_global_ref and arg_kinds_array are alive for the duration of this call.
        let call_args: [jvalue; 9] = [
            jvalue {
                l: udf_jobject.as_raw(),
            },
            jvalue { j: array_arr_addr },
            jvalue { j: array_sch_addr },
            jvalue { j: scalar_arr_addr },
            jvalue { j: scalar_sch_addr },
            jvalue {
                l: arg_kinds_array.as_raw(),
            },
            jvalue { j: result_arr_addr },
            jvalue { j: result_sch_addr },
            jvalue { i: expected_rows },
        ];

        let call_result = unsafe {
            env.call_static_method_unchecked(
                &self.bridge_class,
                self.invoke_method,
                ReturnType::Primitive(Primitive::Byte),
                &call_args,
            )
        };

        // 7. Java-exception path: translate to DataFusionError.
        if env.exception_check().unwrap_or(false) {
            let throwable = env.exception_occurred().map_err(|e| {
                DataFusionError::Execution(format!("exception_occurred failed: {}", e))
            })?;
            env.exception_clear().ok();
            let message = crate::jni_util::jthrowable_to_string(
                &mut env,
                &throwable,
                "UDF",
                &self.name,
                self.verbosity,
            );
            return Err(DataFusionError::Execution(message));
        }

        let result_kind: jbyte = call_result
            .map_err(|e| DataFusionError::Execution(format!("JNI call failed: {}", e)))?
            .b()
            .map_err(|e| {
                DataFusionError::Execution(format!("invokeScalarUdf return decode failed: {}", e))
            })?;

        // 8. Import the result vector. from_ffi consumes the FFI_ArrowArray.
        let result_array_ffi = *result_arr_box;
        let result_schema_ffi = *result_sch_box;
        // SAFETY: bridge populated both structs via Arrow C Data Interface; the exception check
        // above confirmed no Java exception, so the FFI structs are fully initialised.
        let result_data = unsafe { from_ffi(result_array_ffi, &result_schema_ffi) }
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        // 9. Validate type.
        if result_data.data_type() != self.return_field.data_type() {
            return Err(DataFusionError::Execution(format!(
                "Java UDF '{}' returned vector of type {:?}; declared return type was {:?}",
                self.name,
                result_data.data_type(),
                self.return_field.data_type()
            )));
        }

        let array: ArrayRef = make_array(result_data);

        match result_kind {
            0 => Ok(ColumnarValue::Array(array)),
            1 => {
                if array.len() != 1 {
                    return Err(DataFusionError::Internal(format!(
                        "Java UDF '{}' returned Scalar with length {} (expected 1)",
                        self.name,
                        array.len()
                    )));
                }
                let scalar = ScalarValue::try_from_array(&array, 0)?;
                Ok(ColumnarValue::Scalar(scalar))
            }
            other => Err(DataFusionError::Internal(format!(
                "Java UDF '{}' returned unknown kind byte: {}",
                self.name, other
            ))),
        }
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
