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

import org.apache.datafusion.protobuf.MemoryLimit;
import org.apache.datafusion.protobuf.SessionOptions;

/**
 * Builder for a configured {@link SessionContext}. Each setter is optional; unset fields leave the
 * DataFusion default in place.
 */
public final class SessionContextBuilder {
  private Integer batchSize;
  private Integer targetPartitions;
  private Boolean collectStatistics;
  private Boolean informationSchema;
  private Long memoryLimitBytes;
  private Double memoryLimitFraction;
  private String tempDirectory;

  SessionContextBuilder() {}

  public SessionContextBuilder batchSize(int batchSize) {
    this.batchSize = batchSize;
    return this;
  }

  public SessionContextBuilder targetPartitions(int targetPartitions) {
    this.targetPartitions = targetPartitions;
    return this;
  }

  public SessionContextBuilder collectStatistics(boolean collectStatistics) {
    this.collectStatistics = collectStatistics;
    return this;
  }

  public SessionContextBuilder informationSchema(boolean informationSchema) {
    this.informationSchema = informationSchema;
    return this;
  }

  /** Cap the memory pool at {@code maxMemoryBytes}, reserving {@code fraction} of it for queries. */
  public SessionContextBuilder memoryLimit(long maxMemoryBytes, double fraction) {
    this.memoryLimitBytes = maxMemoryBytes;
    this.memoryLimitFraction = fraction;
    return this;
  }

  /** Directory the DiskManager uses for spill files. */
  public SessionContextBuilder tempDirectory(String path) {
    this.tempDirectory = path;
    return this;
  }

  byte[] toBytes() {
    SessionOptions.Builder b = SessionOptions.newBuilder();
    if (batchSize != null) {
      b.setBatchSize(batchSize);
    }
    if (targetPartitions != null) {
      b.setTargetPartitions(targetPartitions);
    }
    if (collectStatistics != null) {
      b.setCollectStatistics(collectStatistics);
    }
    if (informationSchema != null) {
      b.setInformationSchema(informationSchema);
    }
    if (memoryLimitBytes != null && memoryLimitFraction != null) {
      b.setMemoryLimit(
          MemoryLimit.newBuilder()
              .setMaxMemoryBytes(memoryLimitBytes)
              .setMemoryFraction(memoryLimitFraction)
              .build());
    }
    if (tempDirectory != null) {
      b.setTempDirectory(tempDirectory);
    }
    return b.build().toByteArray();
  }
}
