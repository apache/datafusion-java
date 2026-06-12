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

//! Cancellation tokens for in-flight queries.
//!
//! Java handles are opaque `u64` IDs, not raw pointers. A process-global
//! registry maps each live ID to its `Arc<CancellationToken>`. JNI handlers
//! look up by ID and clone the `Arc` out of the registry (under a lock) so a
//! concurrent close can never invalidate a borrow already in flight: the worst
//! a race produces is a clean "closed" error from a missing-ID lookup, never
//! a use-after-free of a freed `Box`.
//!
//! This is the same scaffolding upstream's open close()-race issue calls for
//! across all handle types; it is applied here first because cancellation
//! tokens are designed to be fired from a thread that does not own them, so
//! the race window is the widest of any handle in the binding.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use jni::objects::JClass;
use jni::sys::{jboolean, jlong};
use jni::JNIEnv;
use tokio_util::sync::CancellationToken;

use crate::errors::{try_unwrap_or_throw, JniResult};

fn registry() -> &'static Mutex<HashMap<u64, Arc<CancellationToken>>> {
    static REG: OnceLock<Mutex<HashMap<u64, Arc<CancellationToken>>>> = OnceLock::new();
    REG.get_or_init(|| Mutex::new(HashMap::new()))
}

fn next_id() -> u64 {
    // Start at 1 so jlong 0 stays reserved as the "no token" sentinel on the
    // Java side. Monotonic; 2^64 IDs is enough that reuse is never observed.
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Look up the `Arc<CancellationToken>` for `handle`. Returns `None` if the
/// handle is zero (no token) or has already been closed -- *not* an error,
/// because callers want to distinguish those cases. The cloned `Arc` keeps the
/// inner `CancellationToken` alive for the borrow's lifetime, so a concurrent
/// `closeToken` removing the registry entry is safe: the entry's drop just
/// decrements one of several `Arc` counts.
pub(crate) fn token_arc(handle: jlong) -> Option<Arc<CancellationToken>> {
    if handle == 0 {
        return None;
    }
    let id = handle as u64;
    let guard = registry().lock().expect("cancellation registry poisoned");
    guard.get(&id).cloned()
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_createCancellationToken<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jlong {
    try_unwrap_or_throw(&mut env, 0, |_env| -> JniResult<jlong> {
        let token: Arc<CancellationToken> = Arc::new(CancellationToken::new());
        let id = next_id();
        let mut guard = registry().lock().expect("cancellation registry poisoned");
        guard.insert(id, token);
        Ok(id as jlong)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_CancellationToken_cancelToken<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    try_unwrap_or_throw(&mut env, (), |_env| -> JniResult<()> {
        match token_arc(handle) {
            Some(t) => {
                t.cancel();
                Ok(())
            }
            None => Err("CancellationToken is closed".into()),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_CancellationToken_isCancelledToken<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jboolean {
    try_unwrap_or_throw(&mut env, 0, |_env| -> JniResult<jboolean> {
        match token_arc(handle) {
            Some(t) => Ok(if t.is_cancelled() { 1 } else { 0 }),
            None => Err("CancellationToken is closed".into()),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_CancellationToken_closeToken<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    try_unwrap_or_throw(&mut env, (), |_env| -> JniResult<()> {
        if handle == 0 {
            return Ok(());
        }
        let id = handle as u64;
        // Remove (rather than `drop` a raw Box) -- the underlying Arc may still
        // have outstanding clones held by an in-flight collect/executeStream
        // future, and those keep the inner token alive until they finish.
        let mut guard = registry().lock().expect("cancellation registry poisoned");
        guard.remove(&id);
        Ok(())
    })
}
