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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Setup parameters for constructing a Rerun {@code TableProvider} and registering it on a {@link
 * SessionContext}. Mirrors the Python construction chain {@code
 * CatalogClient(url).get_dataset(name).filter_segments([...]).reader(index=...)}: every field here
 * is "Class 1" (author-set, not query-derived) — pushdown filters and projection are negotiated
 * separately by the Spark connector via DataFusion-proto {@code LogicalPlanNode}s submitted through
 * {@link SessionContext#fromProto}.
 *
 * <p>EXACTLY ONE of {@link Builder#datasetName(String)} or {@link Builder#datasetId(String)} must
 * be set.
 */
public final class RerunTableOptions {

  private final String url;
  private final String datasetName;
  private final String datasetId;
  private final List<String> segments;
  private final String index;
  private final String token;

  private RerunTableOptions(Builder b) {
    if (b.url == null || b.url.isEmpty()) {
      throw new IllegalArgumentException("RerunTableOptions.url must be non-empty");
    }
    if ((b.datasetName == null || b.datasetName.isEmpty())
        == (b.datasetId == null || b.datasetId.isEmpty())) {
      // Both unset or both set — either way is ambiguous.
      throw new IllegalArgumentException(
          "RerunTableOptions: exactly one of datasetName or datasetId must be set");
    }
    this.url = b.url;
    this.datasetName = b.datasetName == null ? "" : b.datasetName;
    this.datasetId = b.datasetId == null ? "" : b.datasetId;
    this.segments =
        b.segments == null ? Collections.emptyList() : Collections.unmodifiableList(b.segments);
    this.index = b.index == null ? "" : b.index;
    this.token = b.token == null ? "" : b.token;
  }

  public static Builder builder() {
    return new Builder();
  }

  public String url() {
    return url;
  }

  public String datasetName() {
    return datasetName;
  }

  public String datasetId() {
    return datasetId;
  }

  public List<String> segments() {
    return segments;
  }

  public String index() {
    return index;
  }

  /**
   * Return a copy with {@code segments} replaced — used by the Spark connector to narrow to one
   * segment per executor task.
   */
  public RerunTableOptions withSegments(List<String> segments) {
    Builder b = new Builder();
    b.url = this.url;
    b.datasetName = this.datasetName.isEmpty() ? null : this.datasetName;
    b.datasetId = this.datasetId.isEmpty() ? null : this.datasetId;
    b.segments = segments == null ? null : new ArrayList<>(segments);
    b.index = this.index.isEmpty() ? null : this.index;
    b.token = this.token.isEmpty() ? null : this.token;
    return new RerunTableOptions(b);
  }

  /**
   * Serialize as the {@code RerunTableOptions} protobuf consumed by the JNI bridge. Public so the
   * Spark connector can ship the bytes through Java serialization to executors (executors
   * deserialize them on their own {@link SessionContext} via {@link
   * SessionContext#registerRerunTable}).
   */
  public byte[] toProtoBytes() {
    org.apache.datafusion.protobuf.RerunTableOptions.Builder b =
        org.apache.datafusion.protobuf.RerunTableOptions.newBuilder()
            .setUrl(url)
            .setDatasetName(datasetName)
            .setDatasetId(datasetId)
            .setIndex(index)
            .setToken(token);
    b.addAllSegments(segments);
    return b.build().toByteArray();
  }

  public static final class Builder {
    private String url;
    private String datasetName;
    private String datasetId;
    private List<String> segments;
    private String index;
    private String token;

    public Builder url(String url) {
      this.url = url;
      return this;
    }

    public Builder datasetName(String name) {
      this.datasetName = name;
      return this;
    }

    public Builder datasetId(String id) {
      this.datasetId = id;
      return this;
    }

    public Builder segments(List<String> segments) {
      this.segments = segments == null ? null : new ArrayList<>(segments);
      return this;
    }

    /** Timeline name to set as the query's {@code filtered_index}. */
    public Builder index(String index) {
      this.index = index;
      return this;
    }

    /** Bearer JWT token. If unset, falls back to stored credentials / {@code REDAP_TOKEN} env. */
    public Builder token(String token) {
      this.token = token;
      return this;
    }

    public RerunTableOptions build() {
      return new RerunTableOptions(this);
    }
  }
}
