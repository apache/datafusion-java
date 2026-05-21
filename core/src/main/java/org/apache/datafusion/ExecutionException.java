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
 * Runtime execution failure: a UDF threw, a join task panicked, an external (non-DataFusion) error
 * propagated up, or an FFI-level failure surfaced. Surfaces upstream {@code
 * DataFusionError::Execution}, {@code ExecutionJoin}, {@code External}, and {@code Ffi}.
 *
 * <p>Note: this is {@code org.apache.datafusion.ExecutionException}, distinct from {@code
 * java.util.concurrent.ExecutionException}. A file that needs both should fully-qualify one or
 * import them with care.
 */
public class ExecutionException extends DataFusionException {

  private static final long serialVersionUID = 1L;

  public ExecutionException(String message) {
    super(message);
  }

  public ExecutionException(String message, Throwable cause) {
    super(message, cause);
  }
}
