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

class ParquetReadOptionsTest {

  @Test
  void defaultsMatchDataFusion() {
    ParquetReadOptions opts = new ParquetReadOptions();
    assertEquals(".parquet", opts.fileExtension());
    assertNull(opts.parquetPruning());
    assertNull(opts.skipMetadata());
    assertNull(opts.metadataSizeHint());
    assertNull(opts.schema());
  }

  @Test
  void fluentSettersChainAndMutate() {
    Schema schema =
        new Schema(
            List.of(
                new Field(
                    "x", FieldType.nullable(new ArrowType.Int(32, true)), null)));

    ParquetReadOptions opts =
        new ParquetReadOptions()
            .fileExtension(".parq")
            .parquetPruning(true)
            .skipMetadata(false)
            .metadataSizeHint(1_048_576L)
            .schema(schema);

    assertEquals(".parq", opts.fileExtension());
    assertEquals(Boolean.TRUE, opts.parquetPruning());
    assertEquals(Boolean.FALSE, opts.skipMetadata());
    assertEquals(Long.valueOf(1_048_576L), opts.metadataSizeHint());
    assertTrue(opts.schema() == schema);
  }

  @Test
  void schemaSetterRetainsReferenceIdentity() {
    Schema schema = new Schema(List.of());
    ParquetReadOptions opts = new ParquetReadOptions().schema(schema);
    assertSame(schema, opts.schema());
  }
}
