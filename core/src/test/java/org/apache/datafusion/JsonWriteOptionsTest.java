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

package org.apache.datafusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.datafusion.protobuf.JsonWriteOptionsProto;
import org.junit.jupiter.api.Test;

class JsonWriteOptionsTest {

  @Test
  void defaultsLeaveEverythingUnset() throws Exception {
    JsonWriteOptionsProto p = JsonWriteOptionsProto.parseFrom(new JsonWriteOptions().toBytes());

    assertFalse(p.hasSingleFileOutput());
    assertEquals(0, p.getPartitionColsCount());
    assertFalse(p.hasFileCompressionType());
  }

  @Test
  void fluentSettersRoundTripThroughProto() throws Exception {
    JsonWriteOptions opts =
        new JsonWriteOptions()
            .singleFileOutput(true)
            .partitionCols("region", "year")
            .fileCompressionType(FileCompressionType.GZIP);

    JsonWriteOptionsProto p = JsonWriteOptionsProto.parseFrom(opts.toBytes());

    assertTrue(p.getSingleFileOutput());
    assertEquals(2, p.getPartitionColsCount());
    assertEquals("region", p.getPartitionCols(0));
    assertEquals("year", p.getPartitionCols(1));
    assertEquals(
        org.apache.datafusion.protobuf.FileCompressionType.FILE_COMPRESSION_TYPE_GZIP,
        p.getFileCompressionType());
  }

  @Test
  void partitionColsResetOnSubsequentCalls() throws Exception {
    JsonWriteOptions opts =
        new JsonWriteOptions().partitionCols("a", "b", "c").partitionCols("only");

    JsonWriteOptionsProto p = JsonWriteOptionsProto.parseFrom(opts.toBytes());
    assertEquals(1, p.getPartitionColsCount());
    assertEquals("only", p.getPartitionCols(0));
  }
}
