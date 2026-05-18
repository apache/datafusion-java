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
use std::sync::Arc;

use datafusion::arrow::array::{make_array, Array, ArrayRef, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::arrow::ffi::{from_ffi, to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use jni::objects::{GlobalRef, JStaticMethodID, JThrowable};
use jni::signature::{Primitive, ReturnType};
use jni::sys::{jlong, jvalue};
use jni::JNIEnv;

pub(crate) struct JavaScalarUdf {
    pub(crate) name: String,
    pub(crate) signature: Signature,
    pub(crate) return_type: DataType,
    /// Global ref to the user's `org.apache.datafusion.ScalarFunction` instance.
    pub(crate) udf_global_ref: GlobalRef,
    /// Global ref to the `org.apache.datafusion.internal.JniBridge` class.
    pub(crate) bridge_class: GlobalRef,
    /// Method ID for `JniBridge.invokeScalarUdf`.
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
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let number_rows = args.number_rows;

        // 1. Materialise scalars to arrays so all columns are length-N.
        let arrays: Vec<ArrayRef> = args
            .args
            .iter()
            .map(|cv| cv.clone().into_array(number_rows))
            .collect::<datafusion::error::Result<Vec<_>>>()?;

        // 2. Build a single struct array carrying all arg columns. Field names/types come
        //    from the signature's Exact type list (matches what the Java caller declared).
        let signature_fields: Vec<Arc<Field>> = match &self.signature.type_signature {
            TypeSignature::Exact(types) => types
                .iter()
                .enumerate()
                .map(|(i, ty)| Arc::new(Field::new(format!("arg{}", i), ty.clone(), true)))
                .collect(),
            _ => {
                return Err(DataFusionError::Internal(
                    "JavaScalarUdf signature is not Exact; only Signature::exact is supported"
                        .to_string(),
                ))
            }
        };

        let fields = Fields::from(
            signature_fields
                .iter()
                .map(|f| f.as_ref().clone())
                .collect::<Vec<Field>>(),
        );
        let struct_array = StructArray::try_new_with_length(fields, arrays, None, number_rows)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        let args_data = struct_array.into_data();
        let (args_ffi_array, args_ffi_schema) =
            to_ffi(&args_data).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        // 3. Pre-allocate empty FFI structs for the result.
        let result_ffi_array = FFI_ArrowArray::empty();
        let result_ffi_schema = FFI_ArrowSchema::empty();

        // 4. Box for stable addresses across the JNI call.
        let mut args_array_box = Box::new(args_ffi_array);
        let mut args_schema_box = Box::new(args_ffi_schema);
        let mut result_array_box = Box::new(result_ffi_array);
        let mut result_schema_box = Box::new(result_ffi_schema);

        let args_array_addr = args_array_box.as_mut() as *mut _ as jlong;
        let args_schema_addr = args_schema_box.as_mut() as *mut _ as jlong;
        let result_array_addr = result_array_box.as_mut() as *mut _ as jlong;
        let result_schema_addr = result_schema_box.as_mut() as *mut _ as jlong;

        // 5. Attach JNI to current thread.
        let mut env = crate::jvm()
            .attach_current_thread()
            .map_err(|e| DataFusionError::Execution(format!("JNI attach failed: {}", e)))?;

        // 6. Call JniBridge.invokeScalarUdf(udf, args*, result*, expectedRowCount).
        //
        // Build the jvalue argument array for call_static_method_unchecked.
        // SAFETY: we build the args inline and pass them immediately; the JObject
        // pointed to by udf_global_ref is alive for the duration of this call.
        let expected_rows = i32::try_from(number_rows).map_err(|_| {
            DataFusionError::Execution(format!(
                "batch row count {} exceeds i32::MAX; UDFs cannot handle batches larger than 2^31 - 1 rows",
                number_rows
            ))
        })?;

        let udf_jobject = self.udf_global_ref.as_obj();
        // SAFETY: udf_jobject is derived from a GlobalRef alive for the duration of this
        // function. The raw pointer is only read by the JNI call below, which happens
        // before any code that could drop udf_global_ref.
        let call_args: [jvalue; 6] = [
            // ScalarFunction instance
            jvalue {
                l: udf_jobject.as_raw(),
            },
            // argsArrayAddr
            jvalue { j: args_array_addr },
            // argsSchemaAddr
            jvalue {
                j: args_schema_addr,
            },
            // resultArrayAddr
            jvalue {
                j: result_array_addr,
            },
            // resultSchemaAddr
            jvalue {
                j: result_schema_addr,
            },
            // expectedRowCount
            jvalue { i: expected_rows },
        ];

        let call_result = unsafe {
            env.call_static_method_unchecked(
                &self.bridge_class,
                self.invoke_method,
                ReturnType::Primitive(Primitive::Void),
                &call_args,
            )
        };

        // 7. If Java threw, translate to DataFusionError. Always check exception_check first.
        if env.exception_check().unwrap_or(false) {
            let throwable = env.exception_occurred().map_err(|e| {
                DataFusionError::Execution(format!("exception_occurred failed: {}", e))
            })?;
            env.exception_clear().ok();
            let message = jthrowable_to_string(&mut env, &throwable, &self.name);
            return Err(DataFusionError::Execution(message));
        }
        call_result.map_err(|e| DataFusionError::Execution(format!("JNI call failed: {}", e)))?;

        // 8. Import result. from_ffi consumes the FFI_ArrowArray.
        let result_array = *result_array_box;
        let result_schema = *result_schema_box;
        // SAFETY: Java's `Data.exportVector` populated `result_array_box` and
        // `result_schema_box` in place via the C Data Interface, and the
        // exception check above guarantees the call succeeded without
        // throwing — so the FFI structs are fully initialized.
        let result_data = unsafe { from_ffi(result_array, &result_schema) }
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        // 9. Validate type.
        if result_data.data_type() != &self.return_type {
            return Err(DataFusionError::Execution(format!(
                "Java UDF '{}' returned vector of type {:?}; declared return type was {:?}",
                self.name,
                result_data.data_type(),
                self.return_type
            )));
        }

        let array: ArrayRef = make_array(result_data);
        Ok(ColumnarValue::Array(array))
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

/// Best-effort: extract class name and getMessage() from a Java throwable.
/// Anything that goes wrong collapses to a generic message so we don't
/// double-throw inside an error path.
fn jthrowable_to_string(env: &mut JNIEnv, throwable: &JThrowable, udf_name: &str) -> String {
    let class_name_result = (|| -> jni::errors::Result<String> {
        let class = env.call_method(throwable, "getClass", "()Ljava/lang/Class;", &[])?;
        let class_obj = class.l()?;
        let name = env.call_method(&class_obj, "getName", "()Ljava/lang/String;", &[])?;
        let name_obj = name.l()?;
        let name_str: String = env.get_string(&name_obj.into())?.into();
        Ok(name_str)
    })();
    let class_name = match class_name_result {
        Ok(s) => s,
        Err(_) => {
            // A reflective call itself threw — clear that secondary exception so the
            // thread is in a clean state when we return to the JVM.
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

    format!("Java UDF '{}' threw {}: {}", udf_name, class_name, message)
}
