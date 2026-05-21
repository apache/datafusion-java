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
 * IO-shaped failure: a local filesystem read failed, an object store request failed, or a parquet /
 * arrow / avro decoder reported a malformed file. Surfaces upstream {@code
 * DataFusionError::IoError}, {@code ObjectStore}, {@code ArrowError}, {@code ParquetError}, and
 * {@code AvroError}.
 *
 * <p>Note: this is {@code org.apache.datafusion.IoException} (lowercase {@code o}), distinct from
 * {@code java.io.IOException}. The {@code IoException} spelling matches the orthography of the
 * underlying {@code DataFusionError::IoError} variant; a file that needs both should fully-qualify
 * one.
 */
public class IoException extends DataFusionException {

  private static final long serialVersionUID = 1L;

  public IoException(String message) {
    super(message);
  }

  public IoException(String message, Throwable cause) {
    super(message, cause);
  }
}
