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

/**
 * Controls how detailed the representation of a Java throwable is when a JVM upcall (UDF, {@link
 * TableProvider}) throws and the failure has to cross back into native code.
 *
 * <p>Set on {@link SessionContextBuilder#exceptionVerbosity}. The setting is locked at
 * session-construction time and applies uniformly to every UDF and table provider registered
 * against the session.
 */
public enum ExceptionVerbosity {
  /** Class name + {@code getMessage()} + standard Java stack trace. The default. */
  FULL,
  /**
   * Class name + {@code getMessage()} only. Matches the behaviour of `datafusion-java` before this
   * setting existed; useful when full traces are too verbose for production logs.
   */
  MESSAGE,
  /**
   * Class name only. For callers who treat exception bodies as untrusted user input that shouldn't
   * reach logs.
   */
  NONE;

  /**
   * Byte tag passed through {@code SessionContext.registerScalarUdf} / {@code registerTableNative}
   * to the native side. {@code 1=FULL}, {@code 2=MESSAGE}, {@code 3=NONE}; {@code 0} is reserved as
   * a "bogus" sentinel and rejected by the Rust decoder.
   */
  byte toByte() {
    switch (this) {
      case FULL:
        return 1;
      case MESSAGE:
        return 2;
      case NONE:
        return 3;
      default:
        throw new IllegalStateException("unreachable: unknown ExceptionVerbosity " + this);
    }
  }
}
