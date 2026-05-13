<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Development

## Build prerequisites

- JDK 17 or newer.
- Rust toolchain (stable, installed via [rustup]).
- [`tpchgen-cli`] — only needed to generate test data for the Parquet
  integration test (`cargo install tpchgen-cli`).

Maven is bundled via the `./mvnw` wrapper; no separate Maven install is
required.

[rustup]: https://rustup.rs/
[`tpchgen-cli`]: https://github.com/clflushopt/tpchgen-rs

## Build and test

```sh
make test
```

This builds the native Rust crate and runs the JUnit tests. The steps can
be run individually:

```sh
cd native && cargo build
./mvnw test
```

The native library must be built before running JVM tests.

The first build in a fresh checkout reaches out to
`raw.githubusercontent.com` to fetch the DataFusion `.proto` files used
to generate the `datafusion-proto` Java classes. Subsequent builds are
offline; the `download-maven-plugin` cache under
`~/.m2/repository/.cache/` satisfies them.

## Test data

The Parquet integration test reads TPC-H SF1 data (~345 MB across 8 tables
in Snappy-compressed Parquet). Generate it once with:

```sh
make tpch-data
```

Tests that need this data skip cleanly if it is missing. `make clean`
does **not** remove `tpch-data/` — delete it manually to reclaim the
disk space.

## Repository layout

- `src/` — Java sources and tests.
- `native/` — Rust crate (JNI + Arrow C Data Interface).
- `docs/` — Sphinx documentation source and build scripts.
