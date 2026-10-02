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

//! Full system test of ADBC partitioned execution against the example driver.
//!
//! Exercises the upstream `execute_partitions` / `read_partition` (PR
//! adbc-drivers/datafusion#32, wired in via the [patch] in Cargo.toml) end to
//! end with our custom provider: plan a query, get one descriptor per output
//! partition, then read each partition on its own and assert the union is the
//! whole table -- the same contract the Spark connector relies on.

use adbc_core::{Connection, Database, Driver, Statement};
use adbc_datafusion_example_driver::{ExampleDriver, ExampleTableProvider};
use datafusion::arrow::array::{Array, Int64Array};

#[test]
fn execute_partitions_then_read_each_partition() {
    let mut driver = ExampleDriver::default();
    let db = driver.new_database().expect("new_database");
    let mut conn = db.new_connection().expect("new_connection");

    // Plan the scan; the provider has three partitions.
    let mut stmt = conn.new_statement().expect("new_statement");
    stmt.set_sql_query(format!(
        "SELECT id, name FROM {}",
        ExampleTableProvider::TABLE_NAME
    ))
    .expect("set_sql_query");
    let result = stmt.execute_partitions().expect("execute_partitions");

    assert!(
        result.partitions.len() >= 2,
        "expected multiple partitions, got {}",
        result.partitions.len()
    );

    // Read each partition independently (as a separate executor would) and
    // collect the ids; the union must be exactly the whole table, once each.
    let mut ids = Vec::new();
    for descriptor in &result.partitions {
        let reader = conn.read_partition(descriptor).expect("read_partition");
        for batch in reader {
            let batch = batch.expect("batch");
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id column is Int64");
            for i in 0..column.len() {
                ids.push(column.value(i));
            }
        }
    }

    ids.sort();
    assert_eq!(
        ids,
        vec![1, 2, 3],
        "every row read exactly once across partitions"
    );
}
