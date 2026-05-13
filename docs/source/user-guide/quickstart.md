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

# Quickstart

This page walks through a complete query end-to-end.

## The full example

```java
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.datafusion.DataFrame;
import org.apache.datafusion.SessionContext;

try (var allocator = new RootAllocator();
     var ctx = new SessionContext()) {

    ctx.registerParquet("orders", "/path/to/orders.parquet");

    try (DataFrame df = ctx.sql(
            "SELECT o_orderpriority, COUNT(*) AS n " +
            "FROM orders GROUP BY o_orderpriority");
         ArrowReader reader = df.collect(allocator)) {
        while (reader.loadNextBatch()) {
            var batch = reader.getVectorSchemaRoot();
            // ...
        }
    }
}
```

## Walkthrough

**Allocator.** `RootAllocator` is the Arrow off-heap memory allocator. Every
JVM-side Arrow buffer is tracked under an allocator; when the allocator is
closed, leaked buffers are reported. Use one allocator per query (or one
per application) and close it in a `try`-with-resources.

**Session context.** `SessionContext` is the entry point into DataFusion. It
holds the catalog of registered tables and the query planner. It is
`AutoCloseable` and **not thread-safe** — use one per thread, or guard
access externally.

**Registering data.** `registerParquet(name, path)` reads the file's footer
on call and exposes it under the given table name. See
[Parquet](parquet.md) for the options form.

**SQL.** `ctx.sql("...")` plans the query and returns a `DataFrame`. The
query is not executed until results are pulled.

**Collecting results.** `df.collect(allocator)` starts native execution and
returns an `ArrowReader`. Each `loadNextBatch()` call pulls the next
`VectorSchemaRoot`; iterate until it returns `false`.

**Cleanup.** Both `SessionContext` and `DataFrame` are `AutoCloseable`. Use
`try`-with-resources so native resources and Arrow buffers are released
even on exception.
