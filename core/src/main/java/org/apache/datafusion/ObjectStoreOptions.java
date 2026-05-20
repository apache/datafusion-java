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

import org.apache.datafusion.protobuf.GcsOptions;
import org.apache.datafusion.protobuf.HttpOptions;
import org.apache.datafusion.protobuf.ObjectStoreRegistration;
import org.apache.datafusion.protobuf.S3Options;

/**
 * Description of an {@code object_store::ObjectStore} backend to register on a {@link
 * SessionContext}'s {@code RuntimeEnv}. Pass instances to {@link
 * SessionContextBuilder#registerObjectStore(ObjectStoreOptions)}.
 *
 * <p>Build instances via the per-backend factories: {@link #s3()}, {@link #gcs()}, {@link
 * #http(String)}. Each factory returns a builder whose {@code build()} produces an immutable {@code
 * ObjectStoreOptions} you can register.
 *
 * <p>For the cloud backends (S3 / GCS), the standard SDK environment variables are read as the
 * default <em>floor</em> (e.g. {@code AWS_ACCESS_KEY_ID}, {@code AWS_DEFAULT_REGION}, ECS /
 * web-identity vars; {@code GOOGLE_APPLICATION_CREDENTIALS}). Explicit setters on the per-backend
 * builder always override those env values, so a JVM running on a credentialed host (IAM role, ECS
 * task, GKE workload identity) can register an empty-credentials store and let the SDK's default
 * chain do its job.
 *
 * <p>The URL DataFusion uses to look up the store is either an explicit {@code url(...)} on the
 * builder or, if unset, derived from the typed fields:
 *
 * <ul>
 *   <li>S3 → {@code s3://<bucket>}
 *   <li>GCS → {@code gs://<bucket>}
 *   <li>HTTP → no scheme-default; {@link #http(String)} requires the URL up front. The URL must be
 *       a host root ({@code https://example.com/} or {@code https://example.com}); paths cause
 *       lookup-vs-base-URL collisions in DataFusion's registry. Register one HTTP store per
 *       scheme+host and let SQL paths carry the rest.
 * </ul>
 */
public abstract sealed class ObjectStoreOptions
    permits ObjectStoreOptions.S3, ObjectStoreOptions.Gcs, ObjectStoreOptions.Http {

  private final String url;

  private ObjectStoreOptions(String url) {
    this.url = url;
  }

  /** Begin building an {@link S3} (also covers MinIO, R2, any S3-compatible) registration. */
  public static S3.Builder s3() {
    return new S3.Builder();
  }

  /** Begin building a {@link Gcs} (Google Cloud Storage) registration. */
  public static Gcs.Builder gcs() {
    return new Gcs.Builder();
  }

  /**
   * Begin building an {@link Http} (listing-capable HTTP/WebDAV) registration. The listing-root URL
   * is required because no sensible default exists.
   *
   * @throws IllegalArgumentException if {@code listingUrl} is {@code null}.
   */
  public static Http.Builder http(String listingUrl) {
    if (listingUrl == null) {
      throw new IllegalArgumentException("http listing URL must be non-null");
    }
    return new Http.Builder(listingUrl);
  }

  abstract ObjectStoreRegistration toRegistration();

  /** Amazon S3 (and any S3-compatible endpoint such as MinIO, Cloudflare R2, Wasabi). */
  public static final class S3 extends ObjectStoreOptions {
    private final String bucket;
    private final String region;
    private final String endpoint;
    private final String accessKeyId;
    private final String secretAccessKey;
    private final String sessionToken;
    private final Boolean allowHttp;
    private final Boolean skipSignature;
    private final Boolean imdsv1Fallback;

    private S3(Builder b) {
      super(b.url);
      this.bucket = b.bucket;
      this.region = b.region;
      this.endpoint = b.endpoint;
      this.accessKeyId = b.accessKeyId;
      this.secretAccessKey = b.secretAccessKey;
      this.sessionToken = b.sessionToken;
      this.allowHttp = b.allowHttp;
      this.skipSignature = b.skipSignature;
      this.imdsv1Fallback = b.imdsv1Fallback;
    }

    @Override
    ObjectStoreRegistration toRegistration() {
      S3Options.Builder s3 = S3Options.newBuilder().setBucket(bucket);
      if (region != null) s3.setRegion(region);
      if (endpoint != null) s3.setEndpoint(endpoint);
      if (accessKeyId != null) s3.setAccessKeyId(accessKeyId);
      if (secretAccessKey != null) s3.setSecretAccessKey(secretAccessKey);
      if (sessionToken != null) s3.setSessionToken(sessionToken);
      if (allowHttp != null) s3.setAllowHttp(allowHttp);
      if (skipSignature != null) s3.setSkipSignature(skipSignature);
      if (imdsv1Fallback != null) s3.setImdsv1Fallback(imdsv1Fallback);
      ObjectStoreRegistration.Builder reg = ObjectStoreRegistration.newBuilder().setS3(s3);
      if (super.url != null) reg.setUrl(super.url);
      return reg.build();
    }

    public static final class Builder {
      private String url;
      private String bucket;
      private String region;
      private String endpoint;
      private String accessKeyId;
      private String secretAccessKey;
      private String sessionToken;
      private Boolean allowHttp;
      private Boolean skipSignature;
      private Boolean imdsv1Fallback;

      private Builder() {}

      /**
       * Override the registration URL. Default is {@code s3://<bucket>}; use this to register under
       * a non-default scheme such as {@code s3a://}.
       */
      public Builder url(String url) {
        this.url = url;
        return this;
      }

      /** Bucket name. Required. */
      public Builder bucket(String bucket) {
        this.bucket = bucket;
        return this;
      }

      public Builder region(String region) {
        this.region = region;
        return this;
      }

      /** Override the endpoint URL (e.g. {@code https://minio.internal:9000}). */
      public Builder endpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
      }

      public Builder accessKeyId(String accessKeyId) {
        this.accessKeyId = accessKeyId;
        return this;
      }

      public Builder secretAccessKey(String secretAccessKey) {
        this.secretAccessKey = secretAccessKey;
        return this;
      }

      public Builder sessionToken(String sessionToken) {
        this.sessionToken = sessionToken;
        return this;
      }

      /** Allow plain {@code http://} endpoints (e.g. local MinIO). */
      public Builder allowHttp(boolean allowHttp) {
        this.allowHttp = allowHttp;
        return this;
      }

      /** Skip request signing (anonymous / public-bucket reads). */
      public Builder skipSignature(boolean skipSignature) {
        this.skipSignature = skipSignature;
        return this;
      }

      /** Allow falling back to IMDSv1 for credential discovery on EC2. */
      public Builder imdsv1Fallback(boolean imdsv1Fallback) {
        this.imdsv1Fallback = imdsv1Fallback;
        return this;
      }

      public S3 build() {
        if (bucket == null || bucket.isEmpty()) {
          throw new IllegalArgumentException("S3 ObjectStoreOptions: bucket is required");
        }
        return new S3(this);
      }
    }
  }

  /** Google Cloud Storage. */
  public static final class Gcs extends ObjectStoreOptions {
    private final String bucket;
    private final String serviceAccountKey;
    private final String serviceAccountPath;
    private final String applicationCredentials;

    private Gcs(Builder b) {
      super(b.url);
      this.bucket = b.bucket;
      this.serviceAccountKey = b.serviceAccountKey;
      this.serviceAccountPath = b.serviceAccountPath;
      this.applicationCredentials = b.applicationCredentials;
    }

    @Override
    ObjectStoreRegistration toRegistration() {
      GcsOptions.Builder gcs = GcsOptions.newBuilder().setBucket(bucket);
      if (serviceAccountKey != null) gcs.setServiceAccountKey(serviceAccountKey);
      if (serviceAccountPath != null) gcs.setServiceAccountPath(serviceAccountPath);
      if (applicationCredentials != null) gcs.setApplicationCredentials(applicationCredentials);
      ObjectStoreRegistration.Builder reg = ObjectStoreRegistration.newBuilder().setGcs(gcs);
      if (super.url != null) reg.setUrl(super.url);
      return reg.build();
    }

    public static final class Builder {
      private String url;
      private String bucket;
      private String serviceAccountKey;
      private String serviceAccountPath;
      private String applicationCredentials;

      private Builder() {}

      /** Override the registration URL. Default is {@code gs://<bucket>}. */
      public Builder url(String url) {
        this.url = url;
        return this;
      }

      /** Bucket name. Required. */
      public Builder bucket(String bucket) {
        this.bucket = bucket;
        return this;
      }

      /** Inline service-account JSON. Mutually exclusive with {@link #serviceAccountPath}. */
      public Builder serviceAccountKey(String serviceAccountKey) {
        this.serviceAccountKey = serviceAccountKey;
        return this;
      }

      /**
       * Filesystem path to service-account JSON. Mutually exclusive with {@link
       * #serviceAccountKey}.
       */
      public Builder serviceAccountPath(String serviceAccountPath) {
        this.serviceAccountPath = serviceAccountPath;
        return this;
      }

      /**
       * Filesystem path to the application-default-credentials JSON. Maps to {@code
       * with_application_credentials} on the upstream Rust builder, which takes a path, not inline
       * content.
       */
      public Builder applicationCredentials(String applicationCredentialsPath) {
        this.applicationCredentials = applicationCredentialsPath;
        return this;
      }

      public Gcs build() {
        if (bucket == null || bucket.isEmpty()) {
          throw new IllegalArgumentException("GCS ObjectStoreOptions: bucket is required");
        }
        if (serviceAccountKey != null && serviceAccountPath != null) {
          throw new IllegalArgumentException(
              "GCS ObjectStoreOptions: serviceAccountKey and serviceAccountPath are mutually"
                  + " exclusive");
        }
        return new Gcs(this);
      }
    }
  }

  /** Listing-capable HTTP / WebDAV store. */
  public static final class Http extends ObjectStoreOptions {
    private final Boolean allowHttp;

    private Http(Builder b) {
      super(b.url);
      this.allowHttp = b.allowHttp;
    }

    @Override
    ObjectStoreRegistration toRegistration() {
      HttpOptions.Builder http = HttpOptions.newBuilder();
      if (allowHttp != null) http.setAllowHttp(allowHttp);
      ObjectStoreRegistration.Builder reg = ObjectStoreRegistration.newBuilder().setHttp(http);
      reg.setUrl(super.url);
      return reg.build();
    }

    public static final class Builder {
      private final String url;
      private Boolean allowHttp;

      private Builder(String url) {
        this.url = url;
      }

      public Builder allowHttp(boolean allowHttp) {
        this.allowHttp = allowHttp;
        return this;
      }

      public Http build() {
        return new Http(this);
      }
    }
  }
}
