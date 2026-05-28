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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

/** Drives the three {@link ExceptionVerbosity} settings end-to-end through a throwing UDF. */
class ExceptionVerbosityTest {

  /**
   * Sentinel string we look for in the formatted output. Lives on the throwing UDF's call frame, so
   * a {@code FULL} stack trace must mention this method by name.
   */
  private static final String STACK_FRAME_MARKER = "ExceptionVerbosityTest$BoomUdf.evaluate";

  /** Sentinel message the UDF throws; used to check {@code MESSAGE} / {@code FULL} verbosity. */
  private static final String USER_MESSAGE = "boom-from-test-12345";

  private static final ArrowType.Int INT32 = new ArrowType.Int(32, true);

  static final class BoomUdf implements ScalarFunction {
    @Override
    public String name() {
      return "boom";
    }

    @Override
    public List<Field> argFields() {
      return List.of(new Field("x", FieldType.nullable(INT32), null));
    }

    @Override
    public Field returnField() {
      return new Field("y", FieldType.nullable(INT32), null);
    }

    @Override
    public Volatility volatility() {
      return Volatility.IMMUTABLE;
    }

    @Override
    public ColumnarValue evaluate(BufferAllocator allocator, ScalarFunctionArgs args) {
      throw new IllegalStateException(USER_MESSAGE);
    }
  }

  private static ScalarUdf newBoomUdf() {
    return new ScalarUdf(new BoomUdf());
  }

  // ---- builder + enum surface ------------------------------------------

  @Test
  void enumValuesAreFullMessageNone() {
    // Pin the enum so a future rename / reorder is a deliberate decision, not
    // a silent bytecode-level change for the JNI byte tag.
    assertEquals(3, ExceptionVerbosity.values().length);
    assertEquals(ExceptionVerbosity.FULL, ExceptionVerbosity.valueOf("FULL"));
    assertEquals(ExceptionVerbosity.MESSAGE, ExceptionVerbosity.valueOf("MESSAGE"));
    assertEquals(ExceptionVerbosity.NONE, ExceptionVerbosity.valueOf("NONE"));
  }

  @Test
  void builderRejectsNullVerbosity() {
    assertThrows(
        IllegalArgumentException.class, () -> SessionContext.builder().exceptionVerbosity(null));
  }

  @Test
  void builderAcceptsAllThreeAndReturnsContext() {
    for (ExceptionVerbosity v : ExceptionVerbosity.values()) {
      try (SessionContext ctx = SessionContext.builder().exceptionVerbosity(v).build()) {
        assertNotNull(ctx);
      }
    }
  }

  // ---- end-to-end UDF routing ------------------------------------------

  @Test
  void defaultIsFullStackTrace() {
    // No setter call -- the no-arg constructor and builder-default code paths
    // both have to default to FULL.
    try (SessionContext ctx = new SessionContext()) {
      String message = runBoomUdfMessageOn(ctx);
      assertContains("default verbosity (no-arg ctor)", message, "IllegalStateException");
      assertContains("default verbosity (no-arg ctor)", message, USER_MESSAGE);
      assertContains("default verbosity (no-arg ctor)", message, STACK_FRAME_MARKER);
    }
    String message = runBoomUdfMessage(SessionContext.builder());
    assertContains("default verbosity (builder)", message, "IllegalStateException");
    assertContains("default verbosity (builder)", message, USER_MESSAGE);
    assertContains("default verbosity (builder)", message, STACK_FRAME_MARKER);
  }

  @Test
  void fullIncludesClassMessageAndStackTrace() {
    String message =
        runBoomUdfMessage(SessionContext.builder().exceptionVerbosity(ExceptionVerbosity.FULL));
    assertContains("FULL verbosity", message, "IllegalStateException");
    assertContains("FULL verbosity", message, USER_MESSAGE);
    assertContains("FULL verbosity", message, STACK_FRAME_MARKER);
  }

  @Test
  void messageIncludesClassAndMessageButNoStackTrace() {
    String message =
        runBoomUdfMessage(SessionContext.builder().exceptionVerbosity(ExceptionVerbosity.MESSAGE));
    assertContains("MESSAGE verbosity", message, "IllegalStateException");
    assertContains("MESSAGE verbosity", message, USER_MESSAGE);
    assertDoesNotContain("MESSAGE verbosity", message, STACK_FRAME_MARKER);
    // The "\n\tat ..." prefix is the canonical Java stack-trace marker; it
    // must not appear anywhere in the MESSAGE-verbosity output.
    assertDoesNotContain("MESSAGE verbosity", message, "\n\tat ");
  }

  @Test
  void noneIncludesClassOnly() {
    String message =
        runBoomUdfMessage(SessionContext.builder().exceptionVerbosity(ExceptionVerbosity.NONE));
    assertContains("NONE verbosity", message, "IllegalStateException");
    assertDoesNotContain("NONE verbosity", message, USER_MESSAGE);
    assertDoesNotContain("NONE verbosity", message, STACK_FRAME_MARKER);
  }

  // ---- TableProvider path ----------------------------------------------

  /** TableProvider whose scan() throws a marker-message exception. */
  static final class BoomTableProvider implements TableProvider {
    @Override
    public org.apache.arrow.vector.types.pojo.Schema schema() {
      return new org.apache.arrow.vector.types.pojo.Schema(
          List.of(new Field("id", FieldType.nullable(INT32), null)));
    }

    @Override
    public org.apache.arrow.vector.ipc.ArrowReader scan(BufferAllocator allocator) {
      throw new IllegalStateException(USER_MESSAGE);
    }
  }

  private static final String TP_FRAME_MARKER = "ExceptionVerbosityTest$BoomTableProvider.scan";

  @Test
  void tableProviderFullStackTrace() {
    String message =
        runBoomTableScanMessage(
            SessionContext.builder().exceptionVerbosity(ExceptionVerbosity.FULL));
    assertContains("TP FULL", message, "IllegalStateException");
    assertContains("TP FULL", message, USER_MESSAGE);
    assertContains("TP FULL", message, TP_FRAME_MARKER);
  }

  @Test
  void tableProviderMessageOnly() {
    String message =
        runBoomTableScanMessage(
            SessionContext.builder().exceptionVerbosity(ExceptionVerbosity.MESSAGE));
    assertContains("TP MESSAGE", message, "IllegalStateException");
    assertContains("TP MESSAGE", message, USER_MESSAGE);
    assertDoesNotContain("TP MESSAGE", message, TP_FRAME_MARKER);
  }

  @Test
  void tableProviderClassOnly() {
    String message =
        runBoomTableScanMessage(
            SessionContext.builder().exceptionVerbosity(ExceptionVerbosity.NONE));
    assertContains("TP NONE", message, "IllegalStateException");
    assertDoesNotContain("TP NONE", message, USER_MESSAGE);
    assertDoesNotContain("TP NONE", message, TP_FRAME_MARKER);
  }

  private static String runBoomTableScanMessage(SessionContextBuilder b) {
    try (SessionContext ctx = b.build();
        BufferAllocator allocator = new RootAllocator()) {
      ctx.registerTable("boom_tp", new BoomTableProvider());
      RuntimeException ex =
          assertThrows(
              RuntimeException.class,
              () -> {
                try (DataFrame df = ctx.sql("SELECT id FROM boom_tp");
                    ArrowReader r = df.collect(allocator)) {
                  while (r.loadNextBatch()) {}
                }
              });
      String message = ex.getMessage();
      assertNotNull(message, "expected exception to carry a message");
      return message;
    }
  }

  // ---- helpers ----------------------------------------------------------

  /** Convenience: build a context off the supplied builder, run the UDF, return the message. */
  private static String runBoomUdfMessage(SessionContextBuilder b) {
    try (SessionContext ctx = b.build()) {
      return runBoomUdfMessageOn(ctx);
    }
  }

  /** Register the BoomUdf on {@code ctx} and capture the {@link RuntimeException} message. */
  private static String runBoomUdfMessageOn(SessionContext ctx) {
    try (BufferAllocator allocator = new RootAllocator()) {
      ctx.registerUdf(newBoomUdf());
      RuntimeException ex =
          assertThrows(
              RuntimeException.class,
              () -> {
                try (DataFrame df = ctx.sql("SELECT boom(CAST(1 AS INT))");
                    ArrowReader r = df.collect(allocator)) {
                  while (r.loadNextBatch()) {}
                }
              });
      String message = ex.getMessage();
      assertNotNull(message, "expected exception to carry a message");
      return message;
    }
  }

  private static void assertContains(String label, String haystack, String needle) {
    assertTrue(
        haystack != null && haystack.contains(needle),
        label + ": expected to contain \"" + needle + "\", got: " + haystack);
  }

  private static void assertDoesNotContain(String label, String haystack, String needle) {
    assertFalse(
        haystack != null && haystack.contains(needle),
        label + ": expected to NOT contain \"" + needle + "\", got: " + haystack);
  }
}
