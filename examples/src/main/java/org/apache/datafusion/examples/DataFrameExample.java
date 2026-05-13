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

package org.apache.datafusion.examples;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

import org.apache.datafusion.DataFrame;
import org.apache.datafusion.ParquetWriteOptions;
import org.apache.datafusion.SessionContext;

/**
 * Demonstrates the DataFrame transformation API plus a Parquet round-trip: read CSV → filter /
 * select / rename / distinct → write Parquet → read back.
 */
public final class DataFrameExample {

  private DataFrameExample() {}

  public static void main(String[] args) throws Exception {
    Path workDir = Files.createTempDirectory("datafusion-example");
    Path csv = workDir.resolve("orders.csv");
    Path parquet = workDir.resolve("orders.parquet");

    Files.writeString(
        csv,
        """
        priority,amount,region
        HIGH,100,US
        LOW,25,EU
        HIGH,40,US
        MEDIUM,60,US
        HIGH,75,EU
        LOW,25,EU
        """);

    try (SessionContext ctx = new SessionContext()) {

      try (DataFrame source = ctx.readCsv(csv.toAbsolutePath().toString());
          DataFrame highPriority = source.filter("priority = 'HIGH'");
          DataFrame projected = highPriority.select("region", "amount");
          DataFrame renamed = projected.withColumnRenamed("amount", "value");
          DataFrame deduped = renamed.distinct()) {

        System.out.println("HIGH-priority orders, distinct (region, value):");
        deduped.show();

        deduped.writeParquet(
            parquet.toAbsolutePath().toString(),
            new ParquetWriteOptions().singleFileOutput(true).compression("snappy"));
      }

      try (DataFrame readBack = ctx.readParquet(parquet.toAbsolutePath().toString())) {
        System.out.println("Round-tripped row count: " + readBack.count());
      }
    } finally {
      try (Stream<Path> tree = Files.walk(workDir)) {
        tree.sorted(Comparator.reverseOrder()).forEach(DataFrameExample::deleteSilently);
      }
    }
  }

  private static void deleteSilently(Path p) {
    try {
      Files.deleteIfExists(p);
    } catch (Exception ignored) {
      // best-effort temp cleanup
    }
  }
}
