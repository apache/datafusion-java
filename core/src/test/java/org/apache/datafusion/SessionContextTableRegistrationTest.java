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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SessionContextTableRegistrationTest {

  @Test
  void tableExistsReturnsFalseForUnregisteredTable() {
    try (SessionContext ctx = new SessionContext()) {
      assertFalse(ctx.tableExists("orders"));
    }
  }

  @Test
  void tableExistsReturnsTrueAfterRegisterCsv(@TempDir Path tempDir) throws Exception {
    Path csv = tempDir.resolve("orders.csv");
    Files.writeString(csv, "id,amount\n1,100\n2,200\n");

    try (SessionContext ctx = new SessionContext()) {
      assertFalse(ctx.tableExists("orders"));
      ctx.registerCsv("orders", csv.toAbsolutePath().toString());
      assertTrue(ctx.tableExists("orders"));
    }
  }

  @Test
  void deregisterTableRemovesRegisteredTable(@TempDir Path tempDir) throws Exception {
    Path csv = tempDir.resolve("orders.csv");
    Files.writeString(csv, "id,amount\n1,100\n2,200\n");

    try (SessionContext ctx = new SessionContext()) {
      ctx.registerCsv("orders", csv.toAbsolutePath().toString());
      assertTrue(ctx.tableExists("orders"));

      ctx.deregisterTable("orders");
      assertFalse(ctx.tableExists("orders"));
    }
  }

  @Test
  void deregisterTableIsNoOpForUnregisteredTable() {
    try (SessionContext ctx = new SessionContext()) {
      // must not throw
      ctx.deregisterTable("nonexistent");
    }
  }

  @Test
  void tableExistsThrowsWhenContextIsClosed() {
    SessionContext ctx = new SessionContext();
    ctx.close();
    assertThrows(IllegalStateException.class, () -> ctx.tableExists("orders"));
  }

  @Test
  void deregisterTableThrowsWhenContextIsClosed() {
    SessionContext ctx = new SessionContext();
    ctx.close();
    assertThrows(IllegalStateException.class, () -> ctx.deregisterTable("orders"));
  }
}
