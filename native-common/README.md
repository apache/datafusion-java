<!---
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

# datafusion-jni-common

Shared JNI plumbing for the [Apache DataFusion Java](https://github.com/apache/datafusion-java)
native crates. It holds the pieces every DataFusion-backed `cdylib` loaded into a
JVM needs, factored out so they live in one place.

## Linking model

Each consuming `cdylib` statically links its own copy of this crate, so the
runtime singleton is per-library, not per-process. Nothing here is exported with
`#[no_mangle]`, so linking it into several `cdylib`s loaded in one JVM cannot
collide.

## Status

This crate is an implementation detail of Apache DataFusion Java. Its API may
change between releases to track the needs of the native crates that depend on
it.
