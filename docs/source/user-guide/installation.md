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

# Installation

Apache DataFusion Java has not yet published a release. Until the first
release, the only way to use the library is to build from source.

## Requirements

- **JDK 17 or newer.** Set `JAVA_HOME` to point at it.
- **Rust toolchain (stable).** Install via [rustup].

[rustup]: https://rustup.rs/

## Build from source

```sh
git clone https://github.com/apache/datafusion-java.git
cd datafusion-java
make test
```

`make test` compiles the native Rust crate, then runs the JUnit tests
against it. The native library must be built before the JVM tests can
run.

The first build in a fresh checkout reaches out to
`raw.githubusercontent.com` to fetch the DataFusion `.proto` files used to
generate the `datafusion-proto` Java classes. Subsequent builds are
offline; the `download-maven-plugin` cache under
`~/.m2/repository/.cache/` satisfies them.

For development workflow details — running individual tests, the TPC-H
integration test data, code style, and how to update the underlying
DataFusion version — see the [Contributor Guide](../contributor-guide/development.md).
