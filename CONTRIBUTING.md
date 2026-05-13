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

# Contributing to Apache DataFusion Java

Bug reports, design discussion, and patches are welcome. This project follows
the Apache DataFusion contribution model.

## Filing issues and discussing changes

- File bugs and feature requests on [GitHub issues](https://github.com/apache/datafusion-java/issues).
- For larger or design-level discussion, the mailing list is
  [dev@datafusion.apache.org](mailto:dev@datafusion.apache.org).
- Please open an issue before sending a PR for any significant change so the
  approach can be agreed on first.

## Development workflow

Branch from `main`, write changes with [conventional commit](https://www.conventionalcommits.org/)
messages in the imperative mood (e.g. `feat: add foo`, `fix(native): handle bar`),
and open a pull request targeting `main`.

The first build in a fresh checkout reaches out to `raw.githubusercontent.com`
to fetch the DataFusion protobuf schemas (see *Updating the DataFusion /
protobuf schema version* below). Subsequent builds are offline — the
`download-maven-plugin` cache under `~/.m2/repository/.cache/` satisfies them.

## Build prerequisites

- JDK 17 or newer.
- Rust toolchain (stable, installed via [rustup]).
- [`tpchgen-cli`] — only needed to generate test data for the Parquet
  integration test (`cargo install tpchgen-cli`).

Maven is bundled via the `./mvnw` wrapper; no separate Maven install required.

[rustup]: https://rustup.rs/
[`tpchgen-cli`]: https://github.com/clflushopt/tpchgen-rs

## Build and test

    make test

This builds the native Rust crate and runs the JUnit tests. The steps can be
run individually:

    cd native && cargo build
    ./mvnw test

The native library must be built before running JVM tests.

## Test data

The Parquet integration test reads TPC-H SF1 data (~345 MB across 8 tables in
Snappy-compressed Parquet). Generate it once with:

    make tpch-data

Tests that need this data skip cleanly if it is missing. `make clean` does
**not** remove `tpch-data/` — delete it manually to reclaim the disk space.

## Code style

- Java: run `./mvnw spotless:apply` before committing. CI fails the build if
  formatting drifts.
- Rust: run `cargo fmt` and `cargo clippy --all-targets -- -D warnings` inside
  `native/`.
- New source files need the Apache 2.0 license header. Apache RAT enforces this
  during `verify`.

## Updating the DataFusion / protobuf schema version

Three things must move together when bumping DataFusion:

1. `native/Cargo.toml` — the `datafusion` crate dependency.
2. `pom.xml` — the `<datafusion.version>` Maven property. **Must equal the
   Cargo version**; a mismatch means JVM-built protobuf plans won't deserialize
   on the native side.
3. `pom.xml` — the `<sha512>` checksums on the two `download-maven-plugin`
   executions. These pin the downloaded `.proto` files; the build fails if
   upstream silently re-tags them, which is the desired behavior.

Recipe:

```sh
# 1. Bump the Cargo dep
$EDITOR native/Cargo.toml             # set datafusion = "<new>"
(cd native && cargo update -p datafusion)

# 2. Bump the Maven property to match
$EDITOR pom.xml                       # set <datafusion.version>

# 3. Compute the new SHA-512 hashes for both `.proto` files from the upstream
#    tag you just set in step 2, then paste them into the two <sha512> elements
#    in pom.xml.
NEW=$(grep -m1 -oE '<datafusion.version>[^<]+' pom.xml | cut -d'>' -f2)
curl -sL "https://raw.githubusercontent.com/apache/datafusion/$NEW/datafusion/proto-common/proto/datafusion_common.proto" | shasum -a 512 | awk '{print $1}'
curl -sL "https://raw.githubusercontent.com/apache/datafusion/$NEW/datafusion/proto/proto/datafusion.proto" | shasum -a 512 | awk '{print $1}'
$EDITOR pom.xml                       # paste the two hashes into the <sha512> elements

# Drop the local download cache so the next build re-downloads against the new hashes.
rm -rf ~/.m2/repository/.cache/download-maven-plugin target/proto

# 4. Verify
make && make test
```

The protobuf runtime version (`<protobuf.version>` in `pom.xml`) tracks the
Java ecosystem (security and JDK compatibility), not DataFusion. Bump it
independently when there is a reason.
