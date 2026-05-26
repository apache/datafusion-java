// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Push-mode streaming table backed by a `tokio::mpsc` channel.
//!
//! Companion to PR #65's pull-mode `JavaTableProvider`. Producers (Java
//! threads) push batches into a `TableSink` that owns the sender end of the
//! channel; the registered table holds the receiver end and exposes it as a
//! `StreamingTable` over a single `PartitionStream`. Single-scan: a
//! registered streaming table can be queried at most once.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{make_array, RecordBatch, StructArray};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::catalog::streaming::StreamingTable;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::stream::StreamExt;
use jni::sys::jlong;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

/// Sideband slot for a terminal error reported by `TableSink::fail()`. Shared
/// between the producer (writer) and consumer (partition stream). Using a
/// sideband slot rather than `tx.send(Err(...))` lets `fail()` always
/// terminate immediately: the producer drops the sender so the consumer
/// observes end-of-stream, and the consumer-wrapping stream consults the
/// slot at EOS to surface the error. Otherwise `fail()` would have to call
/// `blocking_send(Err(...))` which deadlocks if the data channel is full
/// and no consumer is reading yet (e.g. failure happens before any query
/// runs against the table).
type TerminalError = Arc<Mutex<Option<DataFusionError>>>;

/// State of the partition-stream receiver. Two reachable values:
///
/// - `Available(Receiver)` — initial state. The first `execute()` call moves
///   it to `Taken`.
/// - `Taken` — a consumer has scanned the table. Subsequent scans throw
///   the single-scan error.
///
/// Note: `close()` and `fail()` do **not** mutate this slot. close() is
/// the happy-path EOF and any queued batches must still be observable by
/// a not-yet-started consumer; fail() merely sets the sideband terminal
/// error. The close-vs-blocked-write wakeup uses a dedicated `closed`
/// notify on the sink (see `TableSinkHandle::closed`).
///
/// **Owned exclusively by `JavaPartitionStream`.** `TableSinkHandle` holds
/// only the `Sender`. If the sink also held an `Arc` to this state, the
/// `Receiver` would outlive the registered table -- a producer that called
/// `write()` after dropping the `SessionContext` would park forever
/// because the channel still has a live receiver but no path to consume.
enum ReceiverState {
    Available(mpsc::Receiver<DfResult<RecordBatch>>),
    Taken,
}

struct JavaPartitionStream {
    schema: SchemaRef,
    rx: Mutex<ReceiverState>,
    /// Set by the producer's `fail()` before the sender is dropped. The
    /// receiver-wrapping stream consults this on end-of-stream and surfaces
    /// it as the terminal item, so the consumer sees the producer's error
    /// even when the data channel was full at fail-time.
    terminal_error: TerminalError,
}

impl fmt::Debug for JavaPartitionStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JavaPartitionStream")
            .field("schema", &self.schema)
            .finish()
    }
}

impl PartitionStream for JavaPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        // We can't return a Result from execute(), so on a re-scan attempt
        // we synthesise an error stream that yields a single Err.
        let receiver = {
            let mut slot = self.rx.lock().expect("JavaPartitionStream lock poisoned");
            let prev = std::mem::replace(&mut *slot, ReceiverState::Taken);
            match prev {
                ReceiverState::Available(rx) => rx,
                ReceiverState::Taken => {
                    let err = DataFusionError::Execution(
                        "streaming table is single-scan and was already consumed; \
                         re-register the table with a new TableSink to scan again"
                            .to_string(),
                    );
                    let stream = futures::stream::once(async move { Err(err) });
                    return Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream));
                }
            }
        };
        // The data path is the receiver stream (yields DfResult<RecordBatch>).
        // After it drains, append a one-shot tail that pulls the terminal
        // error out of the sideband slot (populated by `fail()`) and emits
        // it as the final Err item, or nothing if the producer closed
        // cleanly. Both halves yield `DfResult<RecordBatch>` so they can be
        // chained directly.
        let terminal = Arc::clone(&self.terminal_error);
        let data_stream = ReceiverStream::new(receiver);
        let terminal_stream = futures::stream::unfold(Some(terminal), |state| async move {
            let terminal = state?;
            let maybe_err = terminal
                .lock()
                .expect("terminal_error lock poisoned")
                .take();
            // Map into the (item, next-state) tuple the unfold expects:
            // emit one terminal-error item and end, or end immediately.
            maybe_err.map(|err| (Err::<RecordBatch, _>(err), None))
        });
        let stream = StreamExt::chain(data_stream, terminal_stream);
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream))
    }
}

/// Public-shaped handle owned by the Java `TableSink`.
///
/// Lifecycle slots:
///
/// - `tx` (`Mutex<Option<Sender>>`): `close()` / `fail()` take it out so
///   subsequent `write()` calls fail synchronously with "sink closed". A
///   `write()` already in flight may have already cloned the sender before
///   `close()` ran; the cloned sender keeps the channel alive until the
///   parked `send` wakes.
/// - `closed_flag` (`AtomicBool`): durable close signal. Set to `true` by
///   `close()` / `fail()` once and never cleared. `write()` checks it
///   *after* registering its `Notify::notified()` future, which closes the
///   lost-wakeup window inherent in `Notify::notify_waiters` (which only
///   delivers permits to waiters that are *already* registered). The
///   `AtomicBool` is what `notify_waiters` is missing: persistence.
/// - `closed_notify` (`Notify`): wakes any `write()` parked on backpressure.
///   `notify_waiters()` is fire-and-forget, so the `closed_flag` re-check
///   after registering the `Notified` future is load-bearing -- without
///   it, a `write()` preempted between cloning `tx` and constructing the
///   future would miss the wake and park forever on a full channel with
///   no consumer.
pub(crate) struct TableSinkHandle {
    schema: SchemaRef,
    tx: Mutex<Option<mpsc::Sender<DfResult<RecordBatch>>>>,
    /// Durable close flag. Set once by close() / fail(); checked by write()
    /// after registering its Notified future to defeat the lost-wakeup race.
    closed_flag: Arc<AtomicBool>,
    /// Wakes any `write()` parked on backpressure. Used together with
    /// `closed_flag`: writers register a `notified()` future first and then
    /// re-check the flag, so any `notify_waiters()` raised between the two
    /// either delivers the wake (because the future is already registered)
    /// or is observed by the flag re-check.
    closed_notify: Arc<tokio::sync::Notify>,
    /// Sideband slot for the terminal error reported by `fail()`. Shared
    /// with the receiver-wrapping stream in `JavaPartitionStream::execute`.
    terminal_error: TerminalError,
}

impl TableSinkHandle {
    fn new(
        schema: SchemaRef,
        tx: mpsc::Sender<DfResult<RecordBatch>>,
        terminal_error: TerminalError,
    ) -> Self {
        Self {
            schema,
            tx: Mutex::new(Some(tx)),
            closed_flag: Arc::new(AtomicBool::new(false)),
            closed_notify: Arc::new(tokio::sync::Notify::new()),
            terminal_error,
        }
    }

    /// Send a batch through the channel, blocking on backpressure. The schema
    /// of the imported `RecordBatch` is validated against the registered
    /// schema before sending; a mismatch fails synchronously without
    /// consuming channel capacity.
    ///
    /// Internally enters the shared Tokio runtime to `select!` between the
    /// actual send and a close signal raised by `close()` / `fail()`.
    ///
    /// Lost-wakeup defence: `Notify::notify_waiters()` only wakes waiters
    /// that have already registered their `Notified` future at the time of
    /// the call. A naive implementation that creates the future *inside*
    /// `select!` would miss the wake if a concurrent close fired between
    /// the sender clone and the future registration -- the writer would
    /// then park on a full channel with no consumer and no other path to
    /// wake. The fix is to construct the `Notified` future first, *then*
    /// re-check a durable `closed_flag` AtomicBool. Any close that fired
    /// in between either set the flag (caught by the re-check) or notified
    /// the future (caught by the select), so the wakeup cannot be lost.
    pub(crate) fn write(&self, batch: RecordBatch) -> Result<(), String> {
        // The imported batch came through `RecordBatch::from(StructArray)`,
        // which rebuilds the schema as `Schema::new(fields)` -- i.e. it
        // drops top-level Schema metadata even when the C-Data Interface
        // delivered it correctly through field metadata. Compare fields
        // only, then re-attach the registered SchemaRef so the consumer
        // sees the original metadata.
        if batch.schema().fields() != self.schema.fields() {
            return Err(format!(
                "TableSink batch schema {:?} does not match registered schema {:?}",
                batch.schema(),
                self.schema
            ));
        }
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), batch.columns().to_vec())
            .map_err(|e| format!("failed to re-attach registered schema to imported batch: {e}"))?;
        let tx = self
            .tx
            .lock()
            .expect("TableSinkHandle lock poisoned")
            .as_ref()
            .ok_or_else(|| "TableSink is closed".to_string())?
            .clone();
        let closed_flag = Arc::clone(&self.closed_flag);
        let closed_notify = Arc::clone(&self.closed_notify);
        let send_future = async move {
            // Register the notified future BEFORE the closed_flag re-check.
            // Notify's contract: a notification arriving between this
            // construction and the await is delivered to this future.
            let notified = closed_notify.notified();
            tokio::pin!(notified);
            // Acquire ordering pairs with Release in close()/fail() so any
            // notify_waiters() that ran before the flag was set is
            // guaranteed to have delivered to a waiter registered before
            // this load.
            if closed_flag.load(Ordering::Acquire) {
                return Err("TableSink was closed concurrently".to_string());
            }
            tokio::select! {
                send_res = tx.send(Ok(batch)) => send_res.map_err(|_| {
                    "consumer side of streaming table is closed (query cancelled or completed)"
                        .to_string()
                }),
                _ = &mut notified => {
                    Err("TableSink was closed concurrently".to_string())
                }
            }
        };
        // `Runtime::block_on` panics with "Cannot start a runtime from within
        // a runtime" if `write()` is invoked from a thread that is already
        // inside a Tokio worker (e.g. a Java `TableProvider.scan` or UDF
        // callback dispatched by DataFusion's executor while the consumer
        // side of the same JNI library is driving a query). Detect that
        // case via `Handle::try_current()` and use `block_in_place` +
        // `Handle::block_on`, which is the supported pattern for
        // synchronously waiting on a future from inside a multi-thread
        // runtime worker -- it tells the scheduler this worker is about
        // to block so it can spawn a replacement.
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| handle.block_on(send_future)),
            Err(_) => crate::runtime().block_on(send_future),
        }
    }

    /// Drop the sender and notify any `write()` parked on backpressure.
    /// Idempotent. The receiver is left intact: a not-yet-started consumer
    /// is still entitled to read every batch the producer queued before
    /// closing.
    ///
    /// Order is load-bearing: set the durable flag *before* notifying.
    /// `Notify::notify_waiters` only wakes already-registered waiters;
    /// the flag's `Release` ordering pairs with the `Acquire` re-check
    /// inside `write()` so a writer whose `Notified` future hadn't yet
    /// registered when notify_waiters ran is guaranteed to observe the
    /// flag and bail out instead of parking on a full channel.
    pub(crate) fn close(&self) {
        let _ = self
            .tx
            .lock()
            .expect("TableSinkHandle lock poisoned")
            .take();
        self.closed_flag.store(true, Ordering::Release);
        self.closed_notify.notify_waiters();
    }

    /// Record a terminal error in the sideband slot, then drop the sender.
    /// The receiver-wrapping stream observes end-of-stream on the channel
    /// and surfaces the sideband error as its final item.
    ///
    /// Doing it this way (sideband slot + sender drop) rather than
    /// `tx.blocking_send(Err(...))` is mandatory: `blocking_send` parks if
    /// the data channel is full and the receiver hasn't been started yet,
    /// which would deadlock the producer in pre-query failure paths
    /// (capacity 1, one successful write, then fail before any consumer).
    pub(crate) fn fail(&self, message: String) -> Result<(), String> {
        let mut tx_slot = self.tx.lock().expect("TableSinkHandle lock poisoned");
        if tx_slot.is_none() {
            return Err("TableSink is already closed".to_string());
        }
        // Stash the error first so the consumer never sees end-of-stream
        // without it.
        *self
            .terminal_error
            .lock()
            .expect("terminal_error lock poisoned") = Some(DataFusionError::Execution(message));
        // Drop the sender, then signal close (flag-then-notify, same
        // ordering as close()). The consumer's ReceiverStream observes EOS
        // once the last sender clone drops; the chained terminal-error
        // tail emits the Err we just stashed.
        tx_slot.take();
        self.closed_flag.store(true, Ordering::Release);
        self.closed_notify.notify_waiters();
        Ok(())
    }
}

/// Construct a `(StreamingTable, TableSinkHandle)` pair sharing an mpsc
/// channel of the given capacity. Caller registers the `StreamingTable` on a
/// `SessionContext` and hands the `TableSinkHandle` back to Java.
pub(crate) fn make_streaming_table(
    schema: SchemaRef,
    capacity: usize,
) -> DfResult<(Arc<StreamingTable>, TableSinkHandle)> {
    let (tx, rx) = mpsc::channel(capacity);
    let terminal_error: TerminalError = Arc::new(Mutex::new(None));
    // The Receiver lives entirely inside JavaPartitionStream. When the
    // registered StreamingTable is dropped (e.g. SessionContext.close()
    // before TableSink.close()), the partition stream drops and the
    // Receiver with it, so any subsequent producer-side `Sender::send`
    // returns Err immediately rather than parking on a dangling channel.
    let partition = Arc::new(JavaPartitionStream {
        schema: Arc::clone(&schema),
        rx: Mutex::new(ReceiverState::Available(rx)),
        terminal_error: Arc::clone(&terminal_error),
    });
    let table = StreamingTable::try_new(Arc::clone(&schema), vec![partition])?;
    Ok((
        Arc::new(table),
        TableSinkHandle::new(schema, tx, terminal_error),
    ))
}

/// Decode a Java-exported batch from a `(FFI_ArrowArray, FFI_ArrowSchema)`
/// pair into a `RecordBatch`. SAFETY: caller must guarantee the two FFI
/// structs were freshly populated by `Data.exportVectorSchemaRoot` and have
/// not yet been imported elsewhere.
///
/// The exported root surfaces as a `StructArray` whose fields are the row
/// columns; `RecordBatch::from(StructArray)` re-projects those into the
/// expected shape.
pub(crate) unsafe fn import_batch_from_ffi(
    array_addr: jlong,
    schema_addr: jlong,
) -> DfResult<RecordBatch> {
    if array_addr == 0 || schema_addr == 0 {
        return Err(DataFusionError::Execution(
            "FFI array or schema address is null".to_string(),
        ));
    }
    // Take ownership of the structs Java populated. From here we are
    // responsible for releasing them via Drop.
    let ffi_array = std::ptr::read(array_addr as *const FFI_ArrowArray);
    let ffi_schema = std::ptr::read(schema_addr as *const FFI_ArrowSchema);
    let array_data = from_ffi(ffi_array, &ffi_schema)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
    let array = make_array(array_data);
    let struct_array = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            DataFusionError::Execution(
                "exported VectorSchemaRoot did not import as StructArray".to_string(),
            )
        })?
        .clone();
    Ok(RecordBatch::from(struct_array))
}
