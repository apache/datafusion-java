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
 * Base unchecked exception for every error surfaced from the native DataFusion side.
 *
 * <p>Concrete subclasses correspond to caller-relevant error categories (planning, execution, IO,
 * resources, configuration, not-implemented). Variants of {@code DataFusionError} on the Rust side
 * that don't fit a clean caller-facing category surface as the parent class itself.
 *
 * <p>All subclasses extend {@link RuntimeException} so existing callers that {@code catch
 * (RuntimeException)} keep working unchanged. Callers that want to discriminate can {@code catch
 * (PlanException)} / {@code catch (ResourcesExhaustedException)} etc., or {@code catch
 * (DataFusionException)} for "anything from DataFusion".
 */
public class DataFusionException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public DataFusionException(String message) {
    super(message);
  }

  public DataFusionException(String message, Throwable cause) {
    super(message, cause);
  }
}
