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

# Java table providers

`SessionContext.registerTable(name, provider)` registers a Java-implemented
table. SQL queries that reference `name` call back into your `TableProvider`
to fetch batches. Data flows from Java to native code via the Arrow C Data
Interface, so there are no extra copies in the hot path. This is the Java
counterpart to DataFusion's Rust `SessionContext::register_table`.

## Implement

```java
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.datafusion.TableProvider;

public final class MyTable implements TableProvider {
    private final Schema schema;

    public MyTable(Schema schema) {
        this.schema = schema;
    }

    @Override public Schema schema() { return schema; }

    @Override
    public ArrowReader scan(BufferAllocator allocator) {
        // Return a fresh ArrowReader. The reader must allocate its buffers
        // from `allocator` (or a child of it) — the framework needs the
        // allocator hierarchy to share a root.
        return openMyReader(allocator);
    }
}
```

For the common case of "I have a schema and a function that returns an
`ArrowReader`," `SimpleTableProvider` packages those two into a ready-made
`TableProvider` without having to subclass:

```java
TableProvider t = new SimpleTableProvider(mySchema(), allocator -> openMyReader(allocator));
ctx.registerTable("t", t);
```

## Register and query

```java
try (SessionContext ctx = new SessionContext();
     BufferAllocator allocator = new RootAllocator()) {
    ctx.registerTable("t", new MyTable(mySchema()));

    try (DataFrame df = ctx.sql("SELECT * FROM t WHERE x > 10");
         ArrowReader r = df.collect(allocator)) {
        while (r.loadNextBatch()) {
            // ...
        }
    }
}
```

## Contract

- `schema()` is called exactly once, on the caller's thread, at registration
  time. Throwing from it aborts registration with the original exception.
- `scan(allocator)` is called once per SQL query that touches the table, on a
  worker thread. It must return a fresh, independent `ArrowReader` on every
  call — this is what makes self-joins and `UNION ALL` over the same table
  work.
- The reader returned by `scan` must allocate its buffers from the supplied
  `allocator` (or a child of it). Arrow Java's `Data.exportArrayStream`
  requires the reader's allocator and the export allocator to share a root.
- The returned reader's schema must equal the schema returned by `schema()`.
  A mismatch fails the query.
- You do not need to close the returned reader yourself. The framework
  installs a release callback that closes it when the underlying FFI stream
  is dropped.

## Errors

Exceptions thrown from `scan()` or from the returned reader surface in the
`RuntimeException` raised by `collect()`. The error message includes the Java
exception class, `getMessage()`, and (at the default `FULL` verbosity) the
Java stack trace. See [scalar-udf.md → Errors](scalar-udf.md#errors) for the
session-wide verbosity setter — it applies uniformly to UDFs and table
providers.

## Threading

`SessionContext` is single-threaded, but `scan(allocator)` may be invoked from
any DataFusion worker thread. If your implementation maintains mutable state
across scans, synchronise it.

## Limitations (v1)

- Single-partition scans only. DataFusion sees the table as one partition;
  multi-partition parallelism is a follow-up.
- No projection or filter pushdown. DataFusion applies projection and
  filters on top of the batches you return; the Java side always sees the
  full schema. The interface is intentionally minimal so it can grow these
  capabilities (as default methods) without breaking existing implementations.
- No `deregisterTable`. Tables live until the `SessionContext` is closed.
