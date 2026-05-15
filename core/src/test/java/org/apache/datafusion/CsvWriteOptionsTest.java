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

import org.apache.datafusion.protobuf.CsvWriteOptionsProto;
import org.apache.datafusion.protobuf.FileCompressionType;
import org.junit.jupiter.api.Test;

class CsvWriteOptionsTest {

  @Test
  void defaultsLeaveEverythingUnset() throws Exception {
    CsvWriteOptionsProto p = CsvWriteOptionsProto.parseFrom(new CsvWriteOptions().toBytes());

    assertFalse(p.hasSingleFileOutput());
    assertEquals(0, p.getPartitionColsCount());
    assertFalse(p.hasHasHeader());
    assertFalse(p.hasDelimiter());
    assertFalse(p.hasQuote());
    assertFalse(p.hasEscape());
    assertFalse(p.hasNullValue());
    assertFalse(p.hasFileCompressionType());
  }

  @Test
  void fluentSettersRoundTripThroughProto() throws Exception {
    CsvWriteOptions opts =
        new CsvWriteOptions()
            .singleFileOutput(true)
            .partitionCols("region", "year")
            .hasHeader(false)
            .delimiter((byte) '|')
            .quote((byte) '\'')
            .escape((byte) '\\')
            .nullValue("\\N")
            .fileCompressionType(CsvReadOptions.FileCompressionType.GZIP);

    CsvWriteOptionsProto p = CsvWriteOptionsProto.parseFrom(opts.toBytes());

    assertTrue(p.getSingleFileOutput());
    assertEquals(2, p.getPartitionColsCount());
    assertEquals("region", p.getPartitionCols(0));
    assertEquals("year", p.getPartitionCols(1));
    assertFalse(p.getHasHeader());
    assertEquals((int) '|', p.getDelimiter());
    assertEquals((int) '\'', p.getQuote());
    assertEquals((int) '\\', p.getEscape());
    assertEquals("\\N", p.getNullValue());
    assertEquals(FileCompressionType.FILE_COMPRESSION_TYPE_GZIP, p.getFileCompressionType());
  }

  @Test
  void partitionColsResetOnSubsequentCalls() throws Exception {
    CsvWriteOptions opts = new CsvWriteOptions().partitionCols("a", "b", "c").partitionCols("only");

    CsvWriteOptionsProto p = CsvWriteOptionsProto.parseFrom(opts.toBytes());
    assertEquals(1, p.getPartitionColsCount());
    assertEquals("only", p.getPartitionCols(0));
  }

  @Test
  void delimiterAcceptsHighByteWithoutSignExtension() throws Exception {
    CsvWriteOptions opts = new CsvWriteOptions().delimiter((byte) 0xC2);
    CsvWriteOptionsProto p = CsvWriteOptionsProto.parseFrom(opts.toBytes());
    assertEquals(0xC2, p.getDelimiter());
  }
}
