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

# SessionContext

`SessionContext` is the entry point into DataFusion from Java. It owns the
catalog of registered tables and the query planner.

## Lifecycle

```java
try (SessionContext ctx = new SessionContext()) {
    // register tables, build queries...
}
```

`SessionContext` is `AutoCloseable`. Closing it releases the underlying
native context. Use `try`-with-resources so the native side is freed even
on exception.

## Threading

A `SessionContext` is **not thread-safe**. Do not share one across threads
without external synchronization. The simplest pattern is one context per
thread.

## What's configurable today

Today, `SessionContext` exposes only data-source registration and query
construction. Tuning knobs that DataFusion offers natively
(`SessionConfig`, `RuntimeEnv`) are not yet wired through the Java API.
See [Project status](project-status.md) for the current shape of the API.
