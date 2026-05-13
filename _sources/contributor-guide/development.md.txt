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

The repository is a multi-module Maven build:

- `pom.xml` — parent POM declaring the `core` and `examples` modules and
  shared plugin/dependency versions.
- `core/` — `datafusion-java` library module (Java sources, tests, and
  generated protobuf classes).
- `examples/` — `datafusion-java-examples` module containing runnable
  examples that depend on the library; built alongside the library so they
  cannot fall out of sync with the API.
- `native/` — Rust crate (JNI + Arrow C Data Interface).
- `proto/` — Protobuf definitions shared between Java and Rust.
- `Makefile` — top-level build orchestration (`make test`, `make tpch-data`).
- `mvnw`, `mvnw.cmd` — bundled Maven wrapper.
- `docs/` — Sphinx documentation source and build scripts.

## Running an example

The examples module wires `exec-maven-plugin` with the right
`java.library.path` and `--add-opens` flags. Install the library to the
local Maven repository once, then run any example by main class:

```sh
./mvnw install -DskipTests
./mvnw -pl :datafusion-java-examples exec:exec \
    -Dexec.mainClass=org.apache.datafusion.examples.SqlQueryExample
```

The bundled examples (under
`examples/src/main/java/org/apache/datafusion/examples/`):

- `SqlQueryExample` — register a CSV and run a SQL aggregation.
- `DataFrameExample` — read CSV → filter / select / rename / distinct →
  write Parquet → read back.
- `ProtoPlanExample` — build a DataFusion `LogicalPlanNode` directly via
  the generated protobuf classes and execute it through
  `SessionContext.fromProto`.

Re-run `mvnw install -DskipTests` whenever you change the library.

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
  Prefix every value with the enum's name (e.g.
  `FILE_COMPRESSION_TYPE_GZIP`, not bare `GZIP`) because proto3 enum
  values are scoped at the package level, and the zero value must be a
  `_UNSPECIFIED` sentinel — the Rust side should reject `UNSPECIFIED`
  rather than silently default it.
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
