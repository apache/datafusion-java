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

//! An end-to-end ADBC driver for a specific, compiled-in DataFusion provider.
//!
//! This is "Part A" of the ADBC route: rather than fork [`adbc-driver-datafusion`]
//! or load providers over `datafusion-ffi`, we depend on it as a library and use
//! its [`DataFusionDriver::new_with_context_init`] hook to register our own
//! [`ExampleTableProvider`] into every database's `SessionContext`. The provider
//! is statically linked; ADBC clients (the arrow-adbc Java driver manager, the
//! Spark connector, Python `adbc_driver_manager`, ...) then query it by SQL or
//! Substrait.
//!
//! [`export_driver!`] emits the C `AdbcDriverInit` entrypoint a driver manager
//! dlopen's. Build the cdylib (`cargo build --release`) and point an ADBC client
//! at the resulting shared library.

mod provider;

use std::sync::Arc;

use adbc_core::error::Result;
use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::Driver;
use adbc_driver_datafusion::{ContextInit, DataFusionDatabase, DataFusionDriver};
use datafusion::prelude::SessionContext;

pub use provider::{ExampleTableProvider, TypesTableProvider};

/// An ADBC driver that registers [`ExampleTableProvider`] into each session.
///
/// A thin wrapper over [`DataFusionDriver`]: it exists only so that `Default`
/// (which [`export_driver!`] calls) constructs the inner driver with our
/// `ContextInit` hook. All `Driver` behavior is delegated.
pub struct ExampleDriver(DataFusionDriver);

impl Default for ExampleDriver {
    fn default() -> Self {
        let init: ContextInit = Arc::new(|ctx: &mut SessionContext, _opts| {
            // Register our provider. A real driver would read connection
            // options out of `_opts` (DatabaseOpts) to decide what to expose.
            ctx.register_table(
                ExampleTableProvider::TABLE_NAME,
                Arc::new(ExampleTableProvider::new()),
            )?;
            // A second table whose schema spans the Arrow types the Spark connector must cast
            // or map (unsigned, ns timestamp, binary, nested list/struct); the E2E test scans
            // it to check schema conversion and source-side arrow_cast with known values.
            ctx.register_table(
                TypesTableProvider::TABLE_NAME,
                Arc::new(TypesTableProvider::new()),
            )?;
            Ok(())
        });
        ExampleDriver(DataFusionDriver::new_with_context_init(None, init))
    }
}

impl Driver for ExampleDriver {
    type DatabaseType = DataFusionDatabase;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        self.0.new_database()
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        self.0.new_database_with_opts(opts)
    }
}

// Export the C ADBC entrypoint. The arrow-adbc driver manager resolves this
// symbol after dlopen'ing the cdylib.
adbc_ffi::export_driver!(AdbcDatafusionExampleInit, ExampleDriver);
