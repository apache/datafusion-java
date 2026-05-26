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

# Code style

To apply Java + Rust formatters in one go:

```sh
make format
```

CI verifies formatting, lint, and license headers on every PR via the
**Lint** and **RAT License Check** workflows.

## Java

Spotless is configured in the parent `pom.xml`. The CI `fmt-check` job
runs:

```sh
./mvnw spotless:check
```

## Rust

The CI `clippy` job runs (from `native/`):

```sh
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
```

`-D warnings` escalates every clippy warning to a build failure.

## License headers

Every tracked source file needs the Apache 2.0 license header. The
**RAT License Check** workflow runs `./mvnw -N apache-rat:check` on every
PR (including docs-only changes). Exclusions live in
`dev/release/rat_exclude_files.txt`.
