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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

class CsvReadOptionsTest {

  @Test
  void defaultsMatchDataFusion() {
    CsvReadOptions opts = new CsvReadOptions();
    assertTrue(opts.hasHeader());
    assertEquals((byte) ',', opts.delimiter());
    assertEquals((byte) '"', opts.quote());
    assertNull(opts.terminator());
    assertNull(opts.escape());
    assertNull(opts.comment());
    assertNull(opts.newlinesInValues());
    assertNull(opts.schemaInferMaxRecords());
    assertEquals(".csv", opts.fileExtension());
    assertEquals(CsvReadOptions.FileCompressionType.UNCOMPRESSED, opts.fileCompressionType());
    assertNull(opts.schema());
  }

  @Test
  void fluentSettersChainAndMutate() {
    Schema schema =
        new Schema(List.of(new Field("x", FieldType.nullable(new ArrowType.Int(32, true)), null)));

    CsvReadOptions opts =
        new CsvReadOptions()
            .hasHeader(false)
            .delimiter((byte) '|')
            .quote((byte) '\'')
            .terminator((byte) '\n')
            .escape((byte) '\\')
            .comment((byte) '#')
            .newlinesInValues(true)
            .schemaInferMaxRecords(10L)
            .fileExtension(".tsv")
            .fileCompressionType(CsvReadOptions.FileCompressionType.GZIP)
            .schema(schema);

    assertEquals(false, opts.hasHeader());
    assertEquals((byte) '|', opts.delimiter());
    assertEquals((byte) '\'', opts.quote());
    assertEquals(Byte.valueOf((byte) '\n'), opts.terminator());
    assertEquals(Byte.valueOf((byte) '\\'), opts.escape());
    assertEquals(Byte.valueOf((byte) '#'), opts.comment());
    assertEquals(Boolean.TRUE, opts.newlinesInValues());
    assertEquals(Long.valueOf(10L), opts.schemaInferMaxRecords());
    assertEquals(".tsv", opts.fileExtension());
    assertEquals(CsvReadOptions.FileCompressionType.GZIP, opts.fileCompressionType());
    assertSame(schema, opts.schema());
  }
}
