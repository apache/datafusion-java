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

# DataFusion as a Spark data source (ADBC)

This repository ships a Spark connector that lets a Spark job read from a
DataFusion `TableProvider` as a native Spark `DataSourceV2`. Spark sees an
ordinary external table; the data is produced by DataFusion in native code and
handed back as Apache Arrow batches.

## What this repository provides

Two pieces work together:

- **The `adbc-datafusion` Spark connector** (`spark/`) — a Spark
  `DataSourceV2` registered under the format name `adbc-datafusion`. It builds
  the scan, pushes projection / filters / limit down, maps DataFusion's scan
  partitions onto Spark input partitions, and imports each partition's Arrow
  data into Spark **zero-copy** as an `ArrowColumnVector`.
- **An example DataFusion ADBC driver** (`examples/adbc-datafusion-driver/`) —
  a small native library (cdylib) that exposes a DataFusion `TableProvider`
  through the [ADBC](https://arrow.apache.org/adbc/) C API. It is a worked
  example you can copy: your own driver registers whatever provider you want and
  is loaded the same way.

The connector is built on the standard Apache Arrow ADBC Java driver manager
(`adbc-core` + `adbc-driver-jni`), so it talks to the native driver through a
stable, public interface. Because that boundary is plain ADBC, the same
connector can front **any** ADBC-speaking source; the DataFusion driver shown
here is just one example, and you can point it at a different ADBC driver
without changing the connector.

```text
Spark DataSourceV2 (adbc-datafusion)
  → arrow-adbc Java driver manager (adbc-core + adbc-driver-jni)
    → native ADBC driver (the DataFusion cdylib)
      → your DataFusion TableProvider
```

## Reading from Spark

Point the `driver` option at the built driver library and the `table` option at
a table the provider exposes:

```scala
val df = spark.read
  .format("adbc-datafusion")
  .option("driver", "/abs/path/to/libadbc_datafusion_example_driver.so")
  .option("entrypoint", "AdbcDatafusionExampleInit")
  .option("table", "example")
  .load()

df.filter("id > 1").select("name").show()
```

The connector probes the schema once (on the driver) when the `DataFrame` is
created, then plans the scan. `df.filter(...)` and `df.select(...)` are pushed
into the DataFusion scan where possible (see below); the rest Spark evaluates
itself.

## Two partitioning models that must be reconciled

The heart of the connector is mapping DataFusion's idea of a partitioned scan
onto Spark's idea of tasks. The two engines size scans on **different
principles**, and a provider author who ignores the difference will get a job
that runs but performs badly.

### DataFusion: parallelism-bound

DataFusion partitions a scan to **saturate the cores of one machine**. Its
built-in `ListingTable`, for example, packs input files into roughly
`target_partitions` groups — where `target_partitions` defaults to the local
core count — using a minimum-size floor and optional single-file splitting only
to *reach* that count. The partition count is therefore approximately

```text
N ≈ target_partitions ≈ cores
```

independent of how much data there is. More data means **bigger** partitions,
not more of them.

### Spark: one task per partition, byte-bound

Spark turns **each input partition into one task**, and a native Spark file
source sizes partitions by **bytes**: it targets roughly one partition per
`spark.sql.files.maxPartitionBytes` (default 128 MB), cutting files into splits
and bin-packing them to that size. (Spark shrinks the target below 128 MB when
the data is small, so that there are at least enough partitions to keep every
core busy.)

So large data means **many** partitions — typically far more than the cluster
has cores — which run in waves. Each task processes a bounded slice (≈128 MB),
which is what keeps memory bounded and lets Spark reschedule stragglers.

### How the connector joins them

For a scan that only pushes projection / filter / limit (no DataFusion-side
join or aggregation), the physical plan's output partitioning **is** the
provider's `scan()` partitioning, and the mapping to Spark is **1:1**:

```text
provider.scan() output partitions
  → DataFusion physical plan partition count   N
  → N ADBC partition descriptors
  → N Spark input partitions = N Spark tasks (scan stage)
```

Spark never splits or merges a partition afterward — `df.rdd().getNumPartitions()
== N`, and the cluster core count only bounds how many of the `N` run at once.

**The consequence you must plan for:** suppose a provider keeps DataFusion's
default of `N = target_partitions ≈ cores` partitions. On a large dataset that
produces only a handful of partitions, each holding about `total_bytes / cores`
of data — so Spark runs a few very large tasks. Because a task is the unit of
work, those oversized tasks bring memory pressure, stragglers that hold up the
whole stage, and no way for Spark to rebalance. To feed Spark well, a provider
should size partitions by **bytes** — the way a Spark file source does — rather
than by the `target_partitions` count.

## Sizing the scan

Aim for the Spark sweet spot:

- **Floor:** `N` ≥ total executor cores, or cores sit idle. `~2–4× cores` gives
  slack for skew and stragglers.
- **Per-partition size:** big enough to amortize per-task overhead — the
  canonical target is **128–256 MB** of data per partition (matching Spark's
  `spark.sql.files.maxPartitionBytes` default of 128 MB).
- **Ceiling — and ADBC lowers it.** Beyond Spark's generic per-task cost, this
  path adds two per-partition costs: each of the `N` ADBC descriptors carries a
  copy of the **whole serialized physical plan**, and each task **deserializes**
  that plan when it reads its partition. (The driver caches a deserialized plan
  per connection, but that does not help here: each Spark task uses its own
  connection and reads a single partition, so the plan is deserialized once per
  task and never reused.) Tens of thousands of tiny partitions is far costlier
  here than for a native file source. Keep `N` in the hundreds; prefer bigger
  partitions over a huge count.

Rule of thumb, balanced by **bytes** rather than split count:

```text
N ≈ clamp(total_bytes / target_bytes, floor=cores, ceiling≈hundreds)
```

## Writing a provider that feeds Spark well

Your provider's `scan()` receives the session, so it can read the connector's
parallelism hint and then decide based on what it knows about the data:

```text
T            = state.config().target_partitions()   // connector hint ≈ desired parallelism
target_bytes = ~128–256 MB                           // bias larger for ADBC to keep N down
min_bytes    = floor so partitions never get tiny    // ~ tens of MB

if total_bytes known AND splittable (files / row groups / key ranges):
    N = clamp(ceil(total_bytes / target_bytes), 1, CEILING)   // byte-bound
    bin-pack splits into N groups BALANCED BY BYTES           // not by count
    do not split below min_bytes; merge small splits

elif split_count known but not bytes (shards / external partitions):
    N = min(split_count, CEILING)                             // natural partitioning
    if split_count >> T: coalesce shards into ~T balanced groups

else (size and splits unknown; opaque / streaming):
    N = T                                                     // fall back to the hint
    lean slightly high — stragglers hurt more than overhead
```

- **Balance by bytes, not by split count.** A task is atomic; Spark cannot
  rebalance a fat partition. One 10 GB partition among 100 small ones stalls the
  whole stage. Bin-pack so partition bytes are even.
- **`target_partitions` is a hint, cap, and fallback** — not the data-bound
  count. Use it when bytes are unknown or as a parallelism ceiling; when you know
  the data size, let **bytes** drive `N`.

### Planning happens once, on the driver

Your provider's `scan()` runs **once**, on the driver, while the multi-partition
descriptors are built. The driver plans the query, fixes the partitioning, and
**serializes the whole physical plan** into each descriptor. Each executor task
then deserializes that plan and executes its partition index. So partition
`index i` always means the same slice: there is no second planning pass that
could disagree with the first, and no need to make `N` stable across a
driver/executor boundary.

The one requirement this places on a provider is that its plan be
**serializable**: built-in DataFusion nodes round-trip through the default codec,
but a custom `ExecutionPlan` node needs a `PhysicalExtensionCodec` registered
with the driver so it can be encoded on the driver and decoded on the executor.

## Connector options

| Option | Required | Meaning |
| --- | --- | --- |
| `driver` | yes | Path to (or manifest name of) the native ADBC driver library. |
| `entrypoint` | depends on driver | Name of the driver's C init symbol (e.g. `AdbcDatafusionExampleInit`). |
| `table` | yes | Table the provider exposes; its schema is probed on the driver. |
| `target_partitions` | no | Parallelism hint. Defaults to `SparkContext.defaultParallelism` (total executor cores). The connector issues `SET datafusion.execution.target_partitions = N` on the planning session, so it influences the partition count when the driver builds the physical plan; that plan (with its partitioning fixed) is what gets serialized into the descriptors. Pass `k × cores` to raise parallelism. |
| `manifest.path` | no | Extra search path for ADBC driver manifests when `driver` is a name rather than an absolute path. |
| *(anything else)* | no | Forwarded verbatim as native ADBC database options, so provider-specific knobs pass straight through. |

Two things are intentionally **not** connector options:

- **`maxPartitionBytes`** — byte-sizing lives in the provider's `scan()`; the
  connector does not know the data size and cannot derive it. Pass a sizing knob
  as a provider-specific option if your provider supports one.
- **Reported partitioning / storage-partitioned joins
  (`SupportsReportPartitioning`)** — not available over ADBC today: the
  descriptor carries no distribution metadata, and DataFusion's hash
  partitioning does not match Spark's key-grouped / bucketed model.

## Summary

- The connector maps the provider's `scan()` partition count **1:1** onto Spark
  tasks.
- DataFusion sizes scans to **cores** (parallelism-bound); Spark sizes them to
  **bytes** (one task per ~128 MB). To feed Spark well, size your provider's
  partitions by bytes — typically 128–256 MB each, floored at executor cores and
  capped in the hundreds because every descriptor carries the full serialized
  plan and each task deserializes it.
- Planning happens once on the driver; the physical plan is serialized into each
  descriptor and executors run it as-is, so a custom `ExecutionPlan` node just
  needs a registered `PhysicalExtensionCodec` to be serializable.
