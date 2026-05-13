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

# User Guide

Apache DataFusion Java is a thin Java binding over the
[Apache DataFusion](https://datafusion.apache.org/) query engine. SQL and
DataFrame queries execute in native Rust; results return to the JVM as
[Apache Arrow](https://arrow.apache.org/) record batches over the Arrow C
Data Interface.

This guide covers installation, the `SessionContext` and `DataFrame` APIs,
and Parquet ingestion.

```{toctree}
:maxdepth: 1

installation
quickstart
sessioncontext
dataframe
parquet
project-status
```

> Early development: no releases yet, API will change. Bug reports and
> contributions welcome.
