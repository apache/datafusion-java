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

import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

/**
 * Shared `next()`/`get()` loop for the connector's columnar readers: each `loadNextBatch()`
 * yields a `VectorSchemaRoot` wrapped as a `ColumnarBatch` of [[NonClosingArrowColumnVector]]s
 * (the reader owns the vectors; Spark must not close them per batch).
 */
private[spark] trait ArrowColumnarBatchIteration {

  /** The Arrow stream this reader drains. Stable for the reader's lifetime. */
  protected def arrowReader: ArrowReader

  private var currentBatch: ColumnarBatch = _

  def next(): Boolean = {
    if (currentBatch != null) {
      currentBatch = null
    }
    if (!arrowReader.loadNextBatch()) return false
    val root = arrowReader.getVectorSchemaRoot
    val vectors: java.util.List[FieldVector] = root.getFieldVectors
    val cols = new Array[ColumnVector](vectors.size())
    var i = 0
    while (i < vectors.size()) {
      cols(i) = new NonClosingArrowColumnVector(vectors.get(i))
      i += 1
    }
    val batch = new ColumnarBatch(cols)
    batch.setNumRows(root.getRowCount)
    currentBatch = batch
    true
  }

  def get(): ColumnarBatch = currentBatch
}
