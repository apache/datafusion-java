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

# Apache DataFusion Java

Java bindings for [Apache DataFusion]. Queries run in native Rust and results
return to the JVM as [Apache Arrow] batches via the Arrow C Data Interface.

[Apache DataFusion]: https://datafusion.apache.org/
[Apache Arrow]: https://arrow.apache.org/

> Early development: no releases yet, API will change. Bug reports and
> contributions welcome.

## Quickstart

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

See the [User Guide](user-guide/index.md) for installation, the DataFrame and
SQL APIs, and Parquet ingestion. See the [Contributor Guide](contributor-guide/index.md)
for build, test, and release workflows.

```{toctree}
:maxdepth: 1
:caption: Links
:hidden:

GitHub Repository <https://github.com/apache/datafusion-java>
Issue Tracker <https://github.com/apache/datafusion-java/issues>
Apache DataFusion <https://datafusion.apache.org/>
Code of Conduct <https://github.com/apache/datafusion/blob/main/CODE_OF_CONDUCT.md>
```

```{toctree}
:maxdepth: 2
:caption: Documentation
:hidden:

User Guide <user-guide/index>
Contributor Guide <contributor-guide/index>
```
