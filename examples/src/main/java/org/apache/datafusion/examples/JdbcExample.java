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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.TimeZone;

import org.apache.arrow.adapter.jdbc.ArrowVectorIterator;
import org.apache.arrow.adapter.jdbc.JdbcToArrow;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfig;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfigBuilder;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.datafusion.DataFrame;
import org.apache.datafusion.SessionContext;
import org.apache.datafusion.TableProvider;

/**
 * Demonstrates a JDBC-backed {@link TableProvider}. Populates an H2 in-memory table, registers it
 * with DataFusion via {@link SessionContext#registerTable}, and runs an aggregation query against
 * it.
 *
 * <p>Run with:
 *
 * <pre>
 *   ./mvnw -pl examples exec:exec -Dexec.mainClass=org.apache.datafusion.examples.JdbcExample
 * </pre>
 */
public final class JdbcExample {

  /**
   * Read-only DataFusion table backed by a JDBC query. Schema is captured at construction time from
   * {@link PreparedStatement#getMetaData()}; each {@link #scan} re-executes the query and streams
   * the result through {@code arrow-jdbc}'s {@link ArrowVectorIterator}.
   */
  public static final class JdbcTableProvider implements TableProvider {
    private final String url;
    private final String query;
    private final Schema schema;

    public JdbcTableProvider(String url, String query) {
      this.url = url;
      this.query = query;
      this.schema = fetchSchema();
    }

    private Schema fetchSchema() {
      try (BufferAllocator tmp = new RootAllocator();
          Connection conn = DriverManager.getConnection(url);
          PreparedStatement stmt = conn.prepareStatement(query)) {
        JdbcToArrowConfig config = configFor(tmp);
        return JdbcToArrowUtils.jdbcToArrowSchema(stmt.getMetaData(), config);
      } catch (SQLException e) {
        throw new RuntimeException("Failed to fetch JDBC schema for query: " + query, e);
      }
    }

    @Override
    public Schema schema() {
      return schema;
    }

    @Override
    public ArrowReader scan(BufferAllocator allocator) {
      // Run the query and stream the result through arrow-jdbc's ArrowVectorIterator, which
      // emits VectorSchemaRoots whose buffers are allocated from `allocator`. We wrap that
      // iterator in a small ArrowReader subclass so that DataFusion can consume it. The
      // JDBC Connection / ResultSet are kept open by the closure created in iteratorOf()
      // and closed by JdbcArrowReader.closeReadSource().
      JdbcToArrowConfig config = configFor(allocator);
      OpenedQuery opened = openQuery(config);
      try {
        ArrowVectorIterator iter = JdbcToArrow.sqlToArrowVectorIterator(opened.rs, config);
        return new JdbcArrowReader(allocator, schema, iter, opened);
      } catch (SQLException | IOException e) {
        opened.closeQuietly();
        throw new RuntimeException("Failed to create JDBC iterator for query: " + query, e);
      }
    }

    private OpenedQuery openQuery(JdbcToArrowConfig config) {
      try {
        Connection conn = DriverManager.getConnection(url);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        return new OpenedQuery(conn, stmt, rs);
      } catch (SQLException e) {
        throw new RuntimeException("Failed to execute JDBC query: " + query, e);
      }
    }

    private static JdbcToArrowConfig configFor(BufferAllocator allocator) {
      return new JdbcToArrowConfigBuilder(
              allocator, Calendar.getInstance(TimeZone.getTimeZone("UTC")))
          .build();
    }
  }

  /** Bundle of JDBC handles to close together when the scan finishes. */
  private static final class OpenedQuery {
    final Connection conn;
    final Statement stmt;
    final ResultSet rs;

    OpenedQuery(Connection conn, Statement stmt, ResultSet rs) {
      this.conn = conn;
      this.stmt = stmt;
      this.rs = rs;
    }

    void closeQuietly() {
      try {
        rs.close();
      } catch (SQLException ignored) {
        // best-effort
      }
      try {
        stmt.close();
      } catch (SQLException ignored) {
        // best-effort
      }
      try {
        conn.close();
      } catch (SQLException ignored) {
        // best-effort
      }
    }
  }

  /**
   * {@link ArrowReader} backed by an {@link ArrowVectorIterator}. Each {@link #loadNextBatch} pulls
   * the next {@link VectorSchemaRoot} from the iterator and transfers its data into the reader's
   * managed root via {@link VectorUnloader}/{@link VectorLoader}.
   */
  private static final class JdbcArrowReader extends ArrowReader {
    private final Schema schema;
    private final ArrowVectorIterator iter;
    private final OpenedQuery opened;

    JdbcArrowReader(
        BufferAllocator allocator, Schema schema, ArrowVectorIterator iter, OpenedQuery opened) {
      super(allocator);
      this.schema = schema;
      this.iter = iter;
      this.opened = opened;
    }

    @Override
    protected Schema readSchema() {
      return schema;
    }

    @Override
    public boolean loadNextBatch() throws IOException {
      if (!iter.hasNext()) {
        return false;
      }
      try (VectorSchemaRoot batch = iter.next()) {
        VectorUnloader unloader = new VectorUnloader(batch);
        try (ArrowRecordBatch rb = unloader.getRecordBatch()) {
          new VectorLoader(getVectorSchemaRoot()).load(rb);
        }
      }
      return true;
    }

    @Override
    public long bytesRead() {
      return 0;
    }

    @Override
    protected void closeReadSource() {
      iter.close();
      opened.closeQuietly();
    }
  }

  public static void main(String[] args) throws Exception {
    String url = "jdbc:h2:mem:demo;DB_CLOSE_DELAY=-1";

    // Populate an H2 in-memory table. Column names are double-quoted so H2 stores them in the
    // exact (lowercase) case; arrow-jdbc uses ResultSetMetaData.getColumnName, which returns
    // the stored case, so the resulting Arrow schema ends up with lowercase field names that
    // DataFusion SQL can refer to without quoting.
    try (Connection conn = DriverManager.getConnection(url);
        Statement stmt = conn.createStatement()) {
      stmt.execute(
          "CREATE TABLE \"orders\" ("
              + "\"id\" INT PRIMARY KEY,"
              + " \"customer\" VARCHAR(64) NOT NULL,"
              + " \"total\" DOUBLE NOT NULL)");
      stmt.execute(
          "INSERT INTO \"orders\" VALUES"
              + " (1, 'alice', 19.99),"
              + " (2, 'bob', 7.50),"
              + " (3, 'alice', 100.00)");
    }

    JdbcTableProvider src =
        new JdbcTableProvider(url, "SELECT \"id\", \"customer\", \"total\" FROM \"orders\"");

    try (SessionContext ctx = new SessionContext();
        BufferAllocator allocator = new RootAllocator()) {
      ctx.registerTable("orders", src);

      try (DataFrame df =
              ctx.sql(
                  "SELECT customer, SUM(total) AS spend"
                      + " FROM orders GROUP BY customer ORDER BY customer");
          ArrowReader reader = df.collect(allocator)) {
        System.out.printf("%-10s | %s%n", "customer", "spend");
        System.out.println("-----------+--------");
        while (reader.loadNextBatch()) {
          VectorSchemaRoot root = reader.getVectorSchemaRoot();
          VarCharVector customer = (VarCharVector) root.getVector("customer");
          Float8Vector spend = (Float8Vector) root.getVector("spend");
          for (int i = 0; i < customer.getValueCount(); i++) {
            System.out.printf("%-10s | %.2f%n", new String(customer.get(i)), spend.get(i));
          }
        }
      }
    }
  }
}
