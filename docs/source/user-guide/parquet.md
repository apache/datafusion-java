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

# Parquet

DataFusion Java reads Parquet through two entry points on `SessionContext`:
`registerParquet` to expose a file as a named table, and `readParquet` to
get a `DataFrame` directly.

## Register a table

```java
ctx.registerParquet("orders", "/path/to/orders.parquet");

try (DataFrame df = ctx.sql("SELECT * FROM orders LIMIT 10")) {
    df.show();
}
```

The file's footer is read at registration time. The table remains in the
catalog for the lifetime of the `SessionContext`.

## Read a DataFrame directly

```java
try (DataFrame df = ctx.readParquet("/path/to/orders.parquet")) {
    df.show();
}
```

`readParquet` skips the catalog and hands back a `DataFrame` straight
away.

## ParquetReadOptions

Both entry points accept a `ParquetReadOptions` to tune the underlying
read. Construct one directly and chain setters:

```java
ParquetReadOptions opts = new ParquetReadOptions()
    .fileExtension(".parquet");

ctx.registerParquet("orders", "/path/to/orders.parquet", opts);
// or
try (DataFrame df = ctx.readParquet("/path/to/orders.parquet", opts)) {
    df.show();
}
```

The supported setters track what DataFusion exposes on its Rust
`ParquetReadOptions` builder. Inspect the class on the Java side for the
exact setters available in the version you are using.
