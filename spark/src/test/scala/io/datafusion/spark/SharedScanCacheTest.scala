/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.datafusion.spark

import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.ArrowReader
import org.scalatest.funsuite.AnyFunSuite

class SharedScanCacheTest extends AnyFunSuite {

  private def spec(scanId: String): SharedScanSpec =
    SharedScanSpec(
      scanId = scanId,
      factoryFqcn = "test.Factory",
      optionsBytes = Array.emptyByteArray,
      projectionColumnNames = Array.empty,
      filterProtoBytes = Array.empty,
      pinnedConfig = PinnedSessionConfig(8, 8192, Vector.empty)
    )

  /** JNI-free fake entry; records close. */
  private final class FakeResources extends SharedScanResources {
    @volatile var closed = false
    override def partitionCount: Int = 3
    override def newTaskAllocator(name: String): BufferAllocator =
      throw new UnsupportedOperationException("not used in cache tests")
    override def openPartitionStream(p: Int, a: BufferAllocator): ArrowReader =
      throw new UnsupportedOperationException("not used in cache tests")
    override def close(): Unit = closed = true
  }

  private final class Fixture {
    val clock = new AtomicLong(0L)
    val buildCount = new AtomicInteger(0)
    var failBuilds = false
    var lastBuilt: FakeResources = _

    val cache = new SharedScanCache(
      buildEntry = _ => {
        buildCount.incrementAndGet()
        if (failBuilds) throw new RuntimeException("synthetic build failure")
        lastBuilt = new FakeResources
        lastBuilt
      },
      nanoClock = () => clock.get()
    )

    def advanceMillis(ms: Long): Unit = clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(ms))
  }

  test("acquire builds once, second acquire reuses, refcount pairs with release") {
    val f = new Fixture
    val r1 = f.cache.acquire(spec("s1"), idleTtlMs = 1000)
    val r2 = f.cache.acquire(spec("s1"), idleTtlMs = 1000)
    assert(f.buildCount.get() == 1)
    assert(r1 eq r2)
    f.cache.release("s1")
    f.cache.release("s1")
  }

  test("concurrent acquires build exactly once") {
    val f = new Fixture
    val n = 8
    val pool = Executors.newFixedThreadPool(n)
    val ready = new CountDownLatch(n)
    val go = new CountDownLatch(1)
    try {
      val futures = (0 until n).map { _ =>
        pool.submit { () =>
          ready.countDown()
          go.await()
          f.cache.acquire(spec("s1"), idleTtlMs = 1000)
        }
      }
      ready.await()
      go.countDown()
      val results = futures.map(_.get(10, TimeUnit.SECONDS))
      assert(f.buildCount.get() == 1)
      assert(results.forall(_ eq results.head))
      (0 until n).foreach(_ => f.cache.release("s1"))
    } finally {
      pool.shutdownNow()
    }
  }

  test("build failure propagates and is not cached") {
    val f = new Fixture
    f.failBuilds = true
    val e = intercept[RuntimeException](f.cache.acquire(spec("s1"), idleTtlMs = 1000))
    assert(e.getMessage == "synthetic build failure")
    f.failBuilds = false
    val r = f.cache.acquire(spec("s1"), idleTtlMs = 1000)
    assert(f.buildCount.get() == 2)
    assert(r eq f.lastBuilt)
    f.cache.release("s1")
  }

  test("idle entry past TTL is evicted and closed") {
    val f = new Fixture
    f.cache.acquire(spec("s1"), idleTtlMs = 1000)
    f.cache.release("s1")
    val built = f.lastBuilt
    f.advanceMillis(999)
    f.cache.evictIdleNow()
    assert(!built.closed)
    f.advanceMillis(2)
    f.cache.evictIdleNow()
    assert(built.closed)
  }

  test("entry in use is never evicted, regardless of idle time") {
    val f = new Fixture
    f.cache.acquire(spec("s1"), idleTtlMs = 1000)
    val built = f.lastBuilt
    f.advanceMillis(100000)
    f.cache.evictIdleNow()
    assert(!built.closed)
    f.cache.release("s1")
    f.advanceMillis(100000)
    f.cache.evictIdleNow()
    assert(built.closed)
  }

  test("release then reacquire within TTL resets idleness") {
    val f = new Fixture
    f.cache.acquire(spec("s1"), idleTtlMs = 1000)
    f.cache.release("s1")
    f.advanceMillis(900)
    // Next task wave lands before TTL: same entry, no rebuild.
    val r = f.cache.acquire(spec("s1"), idleTtlMs = 1000)
    assert(f.buildCount.get() == 1)
    assert(r eq f.lastBuilt)
    f.cache.release("s1")
    f.advanceMillis(900)
    f.cache.evictIdleNow()
    assert(!f.lastBuilt.closed, "idle clock must restart at the last release")
  }

  test("acquire after eviction rebuilds") {
    val f = new Fixture
    f.cache.acquire(spec("s1"), idleTtlMs = 1000)
    f.cache.release("s1")
    val first = f.lastBuilt
    f.advanceMillis(2000)
    f.cache.evictIdleNow()
    assert(first.closed)
    val r = f.cache.acquire(spec("s1"), idleTtlMs = 1000)
    assert(f.buildCount.get() == 2)
    assert(r ne first)
    f.cache.release("s1")
  }

  test("distinct scanIds get distinct entries") {
    val f = new Fixture
    val r1 = f.cache.acquire(spec("s1"), idleTtlMs = 1000)
    val r2 = f.cache.acquire(spec("s2"), idleTtlMs = 1000)
    assert(f.buildCount.get() == 2)
    assert(r1 ne r2)
    f.cache.release("s1")
    f.cache.release("s2")
  }

  test("unbalanced release throws") {
    val f = new Fixture
    intercept[IllegalStateException](f.cache.release("never-acquired"))
  }

  test("shutdown closes everything, even entries in use") {
    val f = new Fixture
    f.cache.acquire(spec("s1"), idleTtlMs = 1000)
    val built = f.lastBuilt
    f.cache.shutdown()
    assert(built.closed)
  }
}
