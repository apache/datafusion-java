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

# Multi-partition execution: upstream driver patch

The Spark connector in this repo can spread a scan across executors using ADBC's
partitioned-execution API (`AdbcStatement.executePartitioned()` →
`AdbcConnection.readPartition(descriptor)`). The Java JNI driver manager binds
both calls, but the **DataFusion driver does not implement them yet**:

```rust
// adbc-driver-datafusion/src/lib.rs (today)
fn execute_partitions(&mut self) -> Result<PartitionedResult> {
    Err(ErrorHelper::not_implemented().message("execute_partitions").to_adbc())
}
fn read_partition(&self, _partition: impl AsRef<[u8]>) -> Result<...> {
    Err(ErrorHelper::not_implemented().message("read_partition").to_adbc())
}
```

So the connector calls `executePartitioned()`, catches `NOT_IMPLEMENTED`, and
falls back to a single `executeQuery()` partition. To get real parallelism, the
driver must produce one ADBC partition per DataFusion output partition. This is
an upstream change to [`adbc-driver-datafusion`](https://github.com/adbc-drivers/datafusion);
the patch below is PR-ready against the structures in its `src/lib.rs`. Until it
lands, point this example's git dependency at a branch carrying it.

## Descriptor: self-contained, by construction

A partition descriptor is shipped to another process and handed back via
`read_partition` with no live handle. It must therefore reconstruct the work on
its own. We encode three things:

```
[u32 LE target_partitions][u32 LE partition_index][u8 kind][query bytes...]
kind 0 = Substrait plan (prost-encoded substrait.proto.Plan)
kind 1 = SQL text (utf-8)
```

The `query bytes` rebuild the logical plan in the executor's `SessionContext`
(which has the same providers registered via `ContextInit`), and
`partition_index` selects the slice.

## The correctness subtlety: deterministic partitioning

`read_partition` re-plans from the query, so partition `i` must mean the **same**
slice it meant when `execute_partitions` counted the partitions. DataFusion
planning is deterministic given the same plan **and the same
`target_partitions`** — but that defaults to the machine's CPU count, which
differs between the driver host and an executor. Left unpinned, the executor
could re-plan into a different partition count and read the wrong (or an
out-of-range) slice.

Fix: capture `target_partitions` at `execute_partitions` time, encode it in every
descriptor, and re-plan in `read_partition` with that value pinned. Both sides
then build the identical physical plan.

## Patch

```rust
// --- new: descriptor codec -------------------------------------------------
use prost::Message; // already a dependency

enum DescQuery { Substrait(Vec<u8>), Sql(String) }

fn encode_query(q: &QueryState) -> adbc_core::error::Result<(u8, Vec<u8>)> {
    match q {
        QueryState::Substrait(plan) => Ok((0, plan.encode_to_vec())),
        QueryState::Sql(sql) => Ok((1, sql.clone().into_bytes())),
        QueryState::Prepared(_) => Err(ErrorHelper::not_implemented()
            .message("partitioned execution of prepared statements")
            .to_adbc()),
    }
}

fn encode_descriptor(target_partitions: u32, index: u32, kind: u8, query: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(9 + query.len());
    out.extend_from_slice(&target_partitions.to_le_bytes());
    out.extend_from_slice(&index.to_le_bytes());
    out.push(kind);
    out.extend_from_slice(query);
    out
}

fn decode_descriptor(bytes: &[u8]) -> adbc_core::error::Result<(usize, u32, DescQuery)> {
    if bytes.len() < 9 {
        return Err(ErrorHelper::invalid_argument().message("short partition descriptor").to_adbc());
    }
    let target_partitions = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let index = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    let query = match bytes[8] {
        0 => DescQuery::Substrait(bytes[9..].to_vec()),
        1 => DescQuery::Sql(String::from_utf8(bytes[9..].to_vec())
            .map_err(|e| ErrorHelper::invalid_argument().message(e.to_string()).to_adbc())?),
        other => return Err(ErrorHelper::invalid_argument()
            .format(format_args!("unknown descriptor kind {other}")).to_adbc()),
    };
    Ok((target_partitions, index, query))
}

// --- DataFusionReader: construct from a single-partition stream ------------
impl DataFusionReader {
    pub fn from_stream(
        runtime: Arc<Runtime>,
        stream: datafusion::execution::SendableRecordBatchStream,
        schema: SchemaRef,
    ) -> Self {
        Self { runtime, stream, schema }
    }
}

// --- DataFusionStatement::execute_partitions -------------------------------
fn execute_partitions(&mut self) -> adbc_core::error::Result<adbc_core::PartitionedResult> {
    let query = self.query.as_ref().ok_or_else(|| {
        ErrorHelper::invalid_state().message("no query or Substrait plan has been set").to_adbc()
    })?;
    self.runtime.block_on(async {
        let df = query.execute(&self.ctx).await?;                 // registers object store, plans logically
        let schema = df.schema().as_arrow().clone();
        let physical = df.create_physical_plan().await.map_err(ErrorHelper::from_datafusion)?;
        let n = physical.output_partitioning().partition_count() as u32;
        let target_partitions = self.ctx.copied_config().target_partitions() as u32;
        let (kind, query_bytes) = encode_query(query)?;
        let partitions = (0..n)
            .map(|i| encode_descriptor(target_partitions, i, kind, &query_bytes))
            .collect::<Vec<_>>();
        Ok(adbc_core::PartitionedResult { partitions, schema, rows_affected: -1 })
    })
}

// --- DataFusionConnection::read_partition ----------------------------------
fn read_partition(
    &self,
    partition: impl AsRef<[u8]>,
) -> adbc_core::error::Result<Box<dyn RecordBatchReader + Send>> {
    let (target_partitions, index, query) = decode_descriptor(partition.as_ref())?;
    self.runtime.block_on(async {
        // Pin target_partitions so the physical plan matches execute_partitions'.
        let state = datafusion::execution::session_state::SessionStateBuilder::new_from_existing(
            self.ctx.state(),
        )
        .with_config(self.ctx.copied_config().with_target_partitions(target_partitions))
        .build();

        let plan = match query {
            DescQuery::Substrait(bytes) => {
                let proto = Plan::decode(bytes.as_slice())
                    .map_err(|e| ErrorHelper::invalid_argument().message(e.to_string()).to_adbc())?;
                from_substrait_plan(&state, &proto).await.map_err(ErrorHelper::from_datafusion)?
            }
            DescQuery::Sql(sql) => {
                state.create_logical_plan(&sql).await.map_err(ErrorHelper::from_datafusion)?
            }
        };
        register_object_store_for_plan(&self.ctx, &plan).await.map_err(ErrorHelper::from_datafusion)?;
        let schema = plan.schema().as_arrow().clone();
        let physical = state.create_physical_plan(&plan).await.map_err(ErrorHelper::from_datafusion)?;
        let stream = physical
            .execute(index as usize, state.task_ctx())
            .map_err(ErrorHelper::from_datafusion)?;
        Ok(Box::new(DataFusionReader::from_stream(self.runtime.clone(), stream, schema.into()))
            as Box<dyn RecordBatchReader + Send>)
    })
}
```

## How the connector consumes it (already implemented here)

`AdbcScanImpl.planInputPartitions` (driver side):

1. build the pushed Substrait plan, `setSubstraitPlan`, call `executePartitioned()`;
2. on success, emit one `AdbcInputPartition` per `PartitionDescriptor`
   (`descriptor == true`, payload = the descriptor bytes);
3. on `NOT_IMPLEMENTED` / `NOT_FOUND`, emit a single plan-carrying partition
   (`descriptor == false`).

`AdbcColumnarPartitionReader` (executor side): for a descriptor partition it calls
`connection.readPartition(payload)`; otherwise `setSubstraitPlan` + `executeQuery`.
Both yield an `ArrowReader` wrapped zero-copy into `ArrowColumnVector`s.

So once this patch lands upstream, the connector lights up multi-partition with
no further change on the Java side.

## Caveats / follow-ups

- `Prepared` statements aren't covered (no portable plan serialization without
  `datafusion-proto`); they return `NOT_IMPLEMENTED`, and the connector falls
  back to single-partition. The connector only sends Substrait, so this is fine.
- `register_object_store_for_plan` is applied in `read_partition` too, so
  object-store-backed providers work on the executor.
- Verify `output_partitioning().partition_count()` against the target source; for
  some providers a `RepartitionExec` may be needed to get N > 1.
