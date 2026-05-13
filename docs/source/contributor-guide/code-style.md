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

## Java

Run the Spotless formatter before committing. CI fails the build if
formatting drifts:

```sh
./mvnw spotless:apply
```

## Rust

Run inside `native/`:

```sh
cargo fmt
cargo clippy --all-targets -- -D warnings
```

`-D warnings` turns clippy warnings into build failures, matching CI.

## License headers

New source files need the Apache 2.0 license header. Apache RAT enforces
this during `verify` — `./mvnw verify` will fail if a tracked file is
missing the header.
