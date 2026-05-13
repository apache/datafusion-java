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

- `pom.xml` — Maven build descriptor.
- `Makefile` — top-level build orchestration (`make test`, `make tpch-data`).
- `mvnw`, `mvnw.cmd` — bundled Maven wrapper.
- `src/` — Java sources and tests.
- `native/` — Rust crate (JNI + Arrow C Data Interface).
- `proto/` — Protobuf definitions shared between Java and Rust.
- `docs/` — Sphinx documentation source and build scripts.

## Passing structured options across the JNI boundary

When a JNI call needs to carry more than a handful of scalar arguments —
for example, a struct of nullable knobs like `CsvReadOptions` or
`SessionOptions` — encode the call's configuration as a protobuf message
rather than expanding the JNI signature with one parameter per field.

Add a `.proto` file under `proto/`, declare `package datafusion_java;`,
and follow the conventions already in use:

- One message per logical bundle of options.
- Use `optional` for fields whose unset-ness must survive the boundary
  (so the Rust side can leave a DataFusion default in place).
- Use proto enums for closed sets of choices instead of strings.
- Keep large opaque payloads (Arrow IPC schemas, plan nodes) as separate
  `byte[]` JNI arguments next to the options proto, not inside it.
- Suffix the message name with `Proto` if a sibling Java class would
  otherwise shadow it (e.g. `CsvReadOptionsProto` vs the public
  `CsvReadOptions`).

The proto is compiled by both `prost-build` (Rust, via `native/build.rs`)
and the Maven `protobuf-maven-plugin` (Java). The Java side builds the
message, serializes to bytes, and passes the byte array through JNI; the
Rust side decodes once and folds the fields into the corresponding
DataFusion struct.

This pattern keeps JNI signatures short, makes nullable and enum fields
explicit in a single typed schema, and lets new fields be added without
touching the signature on either side.
