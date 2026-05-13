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

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.datafusion.DataFrame;
import org.apache.datafusion.SessionContext;

/** Registers a small CSV file and runs a SQL query against it. */
public final class SqlQueryExample {

  private SqlQueryExample() {}

  public static void main(String[] args) throws Exception {
    Path csv = Files.createTempFile("orders", ".csv");
    Files.writeString(
        csv,
        """
        priority,amount
        HIGH,100
        LOW,25
        HIGH,40
        MEDIUM,60
        HIGH,75
        """);

    try (var allocator = new RootAllocator();
        var ctx = new SessionContext()) {
      ctx.registerCsv("orders", csv.toAbsolutePath().toString());

      try (DataFrame df =
              ctx.sql(
                  "SELECT priority, COUNT(*) AS n, SUM(amount) AS total "
                      + "FROM orders GROUP BY priority ORDER BY total DESC");
          ArrowReader reader = df.collect(allocator)) {
        while (reader.loadNextBatch()) {
          VectorSchemaRoot batch = reader.getVectorSchemaRoot();
          System.out.println(batch.contentToTSVString());
        }
      }
    } finally {
      Files.deleteIfExists(csv);
    }
  }
}
