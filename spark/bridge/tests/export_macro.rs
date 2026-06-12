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

//! Compile-level test of `export_bridge!`: the macro must expand to valid
//! `extern "system"` items against a plain builder function. JNI entry
//! points can't be exercised without a live JVM, so the assertion here is
//! that this test crate links with the generated symbols present.

use std::sync::Arc;

use datafusion_spark_bridge::datafusion::arrow::datatypes::Schema;
use datafusion_spark_bridge::datafusion::catalog::TableProvider;
use datafusion_spark_bridge::datafusion::datasource::MemTable;
use datafusion_spark_bridge::{export_bridge, BridgeContext, JniResult};

fn build_provider(
    _ctx: &BridgeContext,
    _options: &[u8],
    _partition: &[u8],
) -> JniResult<Arc<dyn TableProvider>> {
    let schema = Arc::new(Schema::empty());
    let table = MemTable::try_new(schema, vec![vec![]])?;
    Ok(Arc::new(table))
}

export_bridge! {
    jni_class: "com_example_testbridge_BridgeNative",
    build_provider: build_provider,
}

#[test]
fn builder_contract_runs_outside_jvm() {
    // Expansion + linking is the macro test; this additionally runs the
    // builder through the same BridgeContext the expansion hands it.
    let ctx = BridgeContext::get();
    let provider = build_provider(&ctx, &[], &[]).expect("builder failed");
    assert_eq!(provider.schema().fields().len(), 0);
}
