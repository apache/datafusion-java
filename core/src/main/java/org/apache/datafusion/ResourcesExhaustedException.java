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
 * The DataFusion runtime exhausted a configured resource budget — typically the memory pool, but
 * applies to any guard upstream surfaces as {@code DataFusionError::ResourcesExhausted}.
 *
 * <p>Distinguishing this from {@link ExecutionException} lets callers retry transient
 * resource-pressure failures without retrying genuine query bugs.
 */
public class ResourcesExhaustedException extends DataFusionException {

  private static final long serialVersionUID = 1L;

  public ResourcesExhaustedException(String message) {
    super(message);
  }

  public ResourcesExhaustedException(String message, Throwable cause) {
    super(message, cause);
  }
}
