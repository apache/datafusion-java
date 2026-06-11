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

import org.apache.arrow.c.{ArrowArrayStream, Data}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.ArrowReader

/**
 * Arrow C-data import of a native-produced `FFI_ArrowArrayStream`: allocate the empty struct,
 * let the native side write into it, then hand it to Arrow Java. On any failure the struct is
 * released so a half-written stream can't leak.
 */
private[spark] object FfiStream {

  def importReader(allocator: BufferAllocator)(writeStream: Long => Unit): ArrowReader = {
    val stream = ArrowArrayStream.allocateNew(allocator)
    try {
      writeStream(stream.memoryAddress())
      Data.importArrayStream(allocator, stream)
    } catch {
      case t: Throwable =>
        stream.close()
        throw t
    }
  }
}
