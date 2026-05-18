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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DataFrameWriteJsonTest {

  /**
   * Build a 3-row DataFrame {@code (id BIGINT, name VARCHAR)}, write it as JSON via the SUT, then
   * read the result back through {@link SessionContext#registerJson} and run {@code COUNT(*)}.
   * Returns the row count.
   */
  private static long countRowsAt(Path dirOrFile, NdJsonReadOptions readOpts) throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      ctx.registerJson("t", dirOrFile.toAbsolutePath().toString(), readOpts);
      try (DataFrame df = ctx.sql("SELECT COUNT(*) FROM t");
          ArrowReader reader = df.collect(allocator)) {
        assertTrue(reader.loadNextBatch());
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        assertEquals(1, root.getRowCount());
        return ((BigIntVector) root.getVector(0)).get(0);
      }
    }
  }

  /** Materialise a 3-row table the test can hand to {@code writeJson}. */
  private static String threeRowSql() {
    return "SELECT * FROM (VALUES (CAST(1 AS BIGINT), 'alice'),"
        + " (CAST(2 AS BIGINT), 'bob'), (CAST(3 AS BIGINT), 'carol')) AS t(id, name)";
  }

  @Test
  void writeJsonRoundTripsRowCount(@TempDir Path tempDir) throws Exception {
    Path out = tempDir.resolve("out");

    try (SessionContext ctx = new SessionContext();
        DataFrame df = ctx.sql(threeRowSql())) {
      df.writeJson(out.toString());
    }

    assertEquals(3L, countRowsAt(out, new NdJsonReadOptions()));
  }

  @Test
  void writeJsonSingleFileProducesOneFile(@TempDir Path tempDir) throws Exception {
    Path out = tempDir.resolve("out.json");

    try (SessionContext ctx = new SessionContext();
        DataFrame df = ctx.sql(threeRowSql())) {
      df.writeJson(out.toString(), new JsonWriteOptions().singleFileOutput(true));
    }

    assertTrue(Files.isRegularFile(out), "expected single file at " + out);
    assertEquals(3L, countRowsAt(out, new NdJsonReadOptions()));
  }

  @Test
  void writeJsonWithGzipCompressionRoundTrips(@TempDir Path tempDir) throws Exception {
    Path out = tempDir.resolve("gz-out");

    try (SessionContext ctx = new SessionContext();
        DataFrame df = ctx.sql(threeRowSql())) {
      df.writeJson(
          out.toString(), new JsonWriteOptions().fileCompressionType(FileCompressionType.GZIP));
    }

    try (Stream<Path> stream = Files.walk(out)) {
      List<Path> files = stream.filter(Files::isRegularFile).toList();
      assertTrue(!files.isEmpty(), "expected at least one part-file under " + out);
      for (Path p : files) {
        assertTrue(
            p.getFileName().toString().endsWith(".gz"),
            "expected .gz suffix on " + p.getFileName());
      }
    }

    NdJsonReadOptions readOpts =
        new NdJsonReadOptions()
            .fileCompressionType(FileCompressionType.GZIP)
            .fileExtension(".json.gz");
    assertEquals(3L, countRowsAt(out, readOpts));
  }

  @Test
  void writeJsonDefaultsToDirectoryEvenWithExtensionInPath(@TempDir Path tempDir) throws Exception {
    // The Javadoc promises "directory unless overridden via singleFileOutput(true)". DataFusion's
    // own DataFrameWriteOptions defaults to Automatic mode, where an extension in the path
    // (".json" here) silently flips the output to a single file. The native handler explicitly
    // pins the default to directory mode so this contract holds regardless of path shape.
    Path out = tempDir.resolve("out.json");

    try (SessionContext ctx = new SessionContext();
        DataFrame df = ctx.sql(threeRowSql())) {
      df.writeJson(out.toString());
    }

    assertTrue(Files.isDirectory(out), "expected directory output at " + out + ", got a file");
    assertEquals(3L, countRowsAt(out, new NdJsonReadOptions()));
  }

  @Test
  void writeJsonRetainsDataFrame(@TempDir Path tempDir) throws Exception {
    Path out = tempDir.resolve("retained");

    try (SessionContext ctx = new SessionContext();
        DataFrame df = ctx.sql(threeRowSql())) {
      df.writeJson(out.toString());
      assertEquals(3L, df.count());
    }
  }

  @Test
  void writeJsonRejectsNullPath() {
    try (SessionContext ctx = new SessionContext();
        DataFrame df = ctx.sql(threeRowSql())) {
      assertThrows(IllegalArgumentException.class, () -> df.writeJson(null));
    }
  }

  @Test
  void writeJsonRejectsNullOptions(@TempDir Path tempDir) {
    Path out = tempDir.resolve("out");
    try (SessionContext ctx = new SessionContext();
        DataFrame df = ctx.sql(threeRowSql())) {
      assertThrows(IllegalArgumentException.class, () -> df.writeJson(out.toString(), null));
    }
  }
}
