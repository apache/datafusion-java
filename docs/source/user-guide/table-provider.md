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
- `scan(allocator)` is called once per physical scan of the table, on a
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
exception class and `getMessage()`, in the same format used for scalar UDF
errors.

## Threading

`SessionContext` is single-threaded, but `scan(allocator)` may be invoked from
any DataFusion worker thread. If your implementation maintains mutable state
across scans, synchronise it.

## Scan planning and pushdown

Override `TableProvider.scanWithArgs(ScanArgs)` to receive DataFusion's
scan request. The default returns a lazy plan that calls the existing
`scan(allocator)` method, so existing `TableProvider` and
`SimpleTableProvider` implementations need no changes.

```text
DataFusion optimizer
    -> supportsFiltersPushdown(expression protos)
DataFusion physical planning
    -> scanWithArgs(args) -> ExecutionPlan (no source rows read)
DataFusion execution
    -> ExecutionPlan.execute(partition, allocator) -> fresh ArrowReader
```

The request has three immutable components:

| Component | Meaning |
|---|---|
| `projection()` | Optional ordered indices into the registered schema. Absent means all columns; present but empty means zero columns, not zero rows. |
| `filters()` | Supported/inexact filters as generated `LogicalExprNode` protobuf messages, not SQL text. |
| `limit()` | Optional limit supplied by DataFusion's planner. A source may return more matching rows, but must not prematurely truncate before satisfying it. |

`supportsFiltersPushdown` returns one `FilterPushdown` value per expression,
in input order:

- `UNSUPPORTED`: leave evaluation to DataFusion; the filter is not sent to
  `scanWithArgs`.
- `INEXACT`: the source may prune candidates but must retain every matching
  row. DataFusion keeps a residual filter.
- `EXACT`: the source fully applies the predicate with DataFusion-compatible
  semantics; DataFusion can remove its residual filter.

The default declines all filters. Expressions that cannot be represented
by the expression-protobuf boundary are also treated as unsupported.
DataFusion, not the Java adapter, decides whether LIMIT can be pushed
through a residual filter. A provider must not apply additional early
truncation that would discard matching rows.
Limits that cannot fit a non-negative Java `long` are omitted as scan
hints; DataFusion still applies the query's limit.

The provider owns the source-specific planning decisions:

```java
@Override
public ExecutionPlan scanWithArgs(ScanArgs args) {
    // Choose an access path from the request, without reading source rows.
    var accessPath = chooseAccessPath(args);
    return (partition, allocator) -> openProjectedReader(accessPath, allocator);
}
```

The plan may return the full registered schema or the requested projected
schema in the requested order. The native adapter projects full-schema
batches as needed, preserving compatibility with the default `scanWithArgs`.
Already-projected batches are used directly, without projecting them a
second time. Other schema mismatches fail the query. A zero-column reader
must still preserve the number of rows, as required by queries such as
`COUNT(*)`.

Planning/capability callbacks may be repeated. They must not consume
source rows, and must be thread-safe when sharing state. Each execution
opens an independent reader with buffers allocated by the supplied
allocator (or its child). Reader ownership and cleanup are the same as
for a base provider.

This API only supplies a Java-backed leaf plan. It does not expose
arbitrary child-consuming execution operators, install source-specific
optimizer rules, or add source SDK dependencies to the binding.

## Limitations

- Bounded single-partition scans only. DataFusion sees the table as one partition;
  execution receives partition index `0`. The explicit partition argument leaves
  room for future multi-partition support without changing `execute`'s signature.
- The default `scanWithArgs` retains full-schema reads, and
  `supportsFiltersPushdown` defaults to `UNSUPPORTED`. Override these methods
  on `TableProvider` for source-level pushdown.
