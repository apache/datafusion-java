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

package org.apache.datafusion.spark;

import org.apache.spark.sql.connector.read.InputPartition;

/**
 * A serializable slice of an ADBC scan shipped to an executor. Carries only the connector options
 * and an opaque payload -- never a native handle or connection, which are meaningless in another
 * process. Each executor reopens its own ADBC database/connection from {@code options}.
 *
 * <p>The {@code kind} says how the executor turns {@code payload} into an {@code ArrowReader}:
 *
 * <ul>
 *   <li>{@link Kind#DESCRIPTOR}: {@code payload} is an ADBC partition descriptor from {@code
 *       executePartitioned()}; the executor runs {@code AdbcConnection.readPartition(payload)} (the
 *       multi-partition path, one partition per descriptor).
 *   <li>{@link Kind#SUBSTRAIT}: {@code payload} is a serialized Substrait plan; the executor runs
 *       {@code setSubstraitPlan} + {@code executeQuery} (single partition).
 *   <li>{@link Kind#SQL}: {@code payload} is UTF-8 SQL; the executor runs {@code setSqlQuery} +
 *       {@code executeQuery} (single partition; used when the driver/JNI lacks Substrait support).
 * </ul>
 */
final class AdbcInputPartition implements InputPartition {

  private static final long serialVersionUID = 1L;

  enum Kind {
    DESCRIPTOR,
    SUBSTRAIT,
    SQL
  }

  final AdbcOptions options;
  final byte[] payload;
  final Kind kind;

  AdbcInputPartition(AdbcOptions options, byte[] payload, Kind kind) {
    this.options = options;
    this.payload = payload;
    this.kind = kind;
  }
}
