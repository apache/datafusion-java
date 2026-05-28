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

Apache DataFusion Java is published to
[Maven Central](https://central.sonatype.com/artifact/org.apache.datafusion/datafusion-java).
The JAR bundles the native library for Linux (x86_64, aarch64), macOS
(x86_64, aarch64), and Windows (x86_64), so no separate native install is
required on those platforms.

## Requirements

- **JDK 17 or newer.**
- Arrow needs access to `java.nio` internals. Add this to the JVM
  arguments of whatever runs your code:

  ```
  --add-opens=java.base/java.nio=ALL-UNNAMED
  ```

## Maven

```xml
<dependency>
    <groupId>org.apache.datafusion</groupId>
    <artifactId>datafusion-java</artifactId>
    <version>0.1.0</version>
</dependency>
```

## Gradle

```kotlin
dependencies {
    implementation("org.apache.datafusion:datafusion-java:0.1.0")
}
```

Arrow (`arrow-vector`, `arrow-c-data`, `arrow-memory-netty`) is pulled in
transitively — you do not need to declare it yourself.

## Build from source

If you are on a platform without a bundled native library, or want to run
against unreleased changes, build from source:

```sh
git clone https://github.com/apache/datafusion-java.git
cd datafusion-java
make test
```

`make test` compiles the native Rust crate, then runs the JUnit tests
against it. The native library must be built before the JVM tests can
run.

Building from source requires a stable Rust toolchain (install via
[rustup](https://rustup.rs/)) in addition to the JDK.

The first build in a fresh checkout reaches out to
`raw.githubusercontent.com` to fetch the DataFusion `.proto` files used to
generate the `datafusion-proto` Java classes. Subsequent builds are
offline; the `download-maven-plugin` cache under
`~/.m2/repository/.cache/` satisfies them.

For development workflow details — running individual tests, the TPC-H
integration test data, code style, and how to update the underlying
DataFusion version — see the [Contributor Guide](../contributor-guide/development.md).
