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

import java.io.IOException;
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

class DataFrameWriteCsvTest {

  private static Path writeCsv(Path dir, String name, String contents) throws IOException {
    Path file = dir.resolve(name);
    Files.writeString(file, contents);
    return file;
  }

  private static long countRowsAt(Path dirOrFile, CsvReadOptions readOpts) throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      ctx.registerCsv("t", dirOrFile.toAbsolutePath().toString(), readOpts);
      try (DataFrame df = ctx.sql("SELECT COUNT(*) FROM t");
          ArrowReader reader = df.collect(allocator)) {
        assertTrue(reader.loadNextBatch());
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        assertEquals(1, root.getRowCount());
        return ((BigIntVector) root.getVector(0)).get(0);
      }
    }
  }

  @Test
  void writeCsvRoundTripsRowCount(@TempDir Path tempDir) throws Exception {
    Path src = writeCsv(tempDir, "src.csv", "id,name\n1,alice\n2,bob\n3,carol\n");
    Path out = tempDir.resolve("out");

    try (SessionContext ctx = new SessionContext();
        DataFrame df = ctx.readCsv(src.toAbsolutePath().toString())) {
      df.writeCsv(out.toString());
    }

    assertEquals(3L, countRowsAt(out, new CsvReadOptions()));
  }

  @Test
  void writeCsvSingleFileProducesOneFile(@TempDir Path tempDir) throws Exception {
    Path src = writeCsv(tempDir, "src.csv", "id,name\n1,alice\n2,bob\n3,carol\n");
    Path out = tempDir.resolve("out.csv");

    try (SessionContext ctx = new SessionContext();
        DataFrame df = ctx.readCsv(src.toAbsolutePath().toString())) {
      df.writeCsv(out.toString(), new CsvWriteOptions().singleFileOutput(true));
    }

    assertTrue(Files.isRegularFile(out), "expected single file at " + out);
    assertEquals(3L, countRowsAt(out, new CsvReadOptions()));
  }

  @Test
  void writeCsvWithCustomDelimiter(@TempDir Path tempDir) throws Exception {
    Path src = writeCsv(tempDir, "src.csv", "id,name\n1,alice\n2,bob\n");
    Path out = tempDir.resolve("out.csv");

    try (SessionContext ctx = new SessionContext();
        DataFrame df = ctx.readCsv(src.toAbsolutePath().toString())) {
      df.writeCsv(
          out.toString(), new CsvWriteOptions().singleFileOutput(true).delimiter((byte) '|'));
    }

    String written = Files.readString(out);
    assertTrue(written.contains("|"), "expected pipe delimiter in output, got: " + written);

    assertEquals(2L, countRowsAt(out, new CsvReadOptions().delimiter((byte) '|')));
  }

  @Test
  void writeCsvWithGzipCompressionRoundTrips(@TempDir Path tempDir) throws Exception {
    Path src = writeCsv(tempDir, "src.csv", "id,name\n1,alice\n2,bob\n3,carol\n4,dan\n");
    Path out = tempDir.resolve("gz-out");

    try (SessionContext ctx = new SessionContext();
        DataFrame df = ctx.readCsv(src.toAbsolutePath().toString())) {
      df.writeCsv(
          out.toString(),
          new CsvWriteOptions().fileCompressionType(CsvReadOptions.FileCompressionType.GZIP));
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

    CsvReadOptions readOpts =
        new CsvReadOptions()
            .fileCompressionType(CsvReadOptions.FileCompressionType.GZIP)
            .fileExtension(".csv.gz");
    assertEquals(4L, countRowsAt(out, readOpts));
  }

  @Test
  void writeCsvRetainsDataFrame(@TempDir Path tempDir) throws Exception {
    Path src = writeCsv(tempDir, "src.csv", "id,name\n1,alice\n2,bob\n3,carol\n");
    Path out = tempDir.resolve("retained");

    try (SessionContext ctx = new SessionContext();
        DataFrame df = ctx.readCsv(src.toAbsolutePath().toString())) {
      df.writeCsv(out.toString());
      assertEquals(3L, df.count());
    }
  }

  @Test
  void writeCsvRejectsNullPath(@TempDir Path tempDir) throws Exception {
    Path src = writeCsv(tempDir, "src.csv", "id,name\n1,alice\n");
    try (SessionContext ctx = new SessionContext();
        DataFrame df = ctx.readCsv(src.toAbsolutePath().toString())) {
      assertThrows(IllegalArgumentException.class, () -> df.writeCsv(null));
    }
  }

  @Test
  void writeCsvRejectsNullOptions(@TempDir Path tempDir) throws Exception {
    Path src = writeCsv(tempDir, "src.csv", "id,name\n1,alice\n");
    Path out = tempDir.resolve("out");
    try (SessionContext ctx = new SessionContext();
        DataFrame df = ctx.readCsv(src.toAbsolutePath().toString())) {
      assertThrows(IllegalArgumentException.class, () -> df.writeCsv(out.toString(), null));
    }
  }
}
