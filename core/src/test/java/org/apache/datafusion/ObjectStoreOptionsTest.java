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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.apache.datafusion.protobuf.ObjectStoreRegistration;
import org.apache.datafusion.protobuf.SessionOptions;
import org.junit.jupiter.api.Test;

class ObjectStoreOptionsTest {

  @Test
  void s3FullSetterSurfaceRoundTripsThroughProto() throws Exception {
    byte[] bytes =
        SessionContext.builder()
            .registerObjectStore(
                ObjectStoreOptions.s3()
                    .bucket("orders")
                    .region("us-east-1")
                    .endpoint("https://minio.internal:9000")
                    .accessKeyId("AKIA...")
                    .secretAccessKey("...")
                    .sessionToken("...")
                    .allowHttp(true)
                    .skipSignature(false)
                    .imdsv1Fallback(true)
                    .build())
            .toBytes();

    SessionOptions parsed = SessionOptions.parseFrom(bytes);
    List<ObjectStoreRegistration> regs = parsed.getObjectStoresList();
    assertEquals(1, regs.size());
    ObjectStoreRegistration r = regs.get(0);
    assertTrue(r.hasS3());
    assertFalse(r.hasUrl(), "default URL is derived on the Rust side, not sent on the wire");
    assertEquals("orders", r.getS3().getBucket());
    assertEquals("us-east-1", r.getS3().getRegion());
    assertEquals("https://minio.internal:9000", r.getS3().getEndpoint());
    assertEquals("AKIA...", r.getS3().getAccessKeyId());
    assertTrue(r.getS3().getAllowHttp());
    assertFalse(r.getS3().getSkipSignature());
    assertTrue(r.getS3().getImdsv1Fallback());
  }

  @Test
  void s3UnsetFieldsAreAbsentInProto() throws Exception {
    byte[] bytes =
        SessionContext.builder()
            .registerObjectStore(ObjectStoreOptions.s3().bucket("b").build())
            .toBytes();
    SessionOptions parsed = SessionOptions.parseFrom(bytes);
    ObjectStoreRegistration r = parsed.getObjectStores(0);
    assertTrue(r.hasS3());
    assertFalse(r.hasUrl());
    assertEquals("b", r.getS3().getBucket());
    // Every optional must travel as unset, not as zero/empty — the Rust side
    // distinguishes "leave the upstream default in place" from "explicitly 0".
    assertFalse(r.getS3().hasRegion());
    assertFalse(r.getS3().hasEndpoint());
    assertFalse(r.getS3().hasAccessKeyId());
    assertFalse(r.getS3().hasSecretAccessKey());
    assertFalse(r.getS3().hasSessionToken());
    assertFalse(r.getS3().hasAllowHttp());
    assertFalse(r.getS3().hasSkipSignature());
    assertFalse(r.getS3().hasImdsv1Fallback());
  }

  @Test
  void s3UrlOverrideRidesOnTheRegistrationNotTheBackendMessage() throws Exception {
    byte[] bytes =
        SessionContext.builder()
            .registerObjectStore(ObjectStoreOptions.s3().bucket("b").url("s3a://b").build())
            .toBytes();
    SessionOptions parsed = SessionOptions.parseFrom(bytes);
    ObjectStoreRegistration r = parsed.getObjectStores(0);
    assertTrue(r.hasUrl());
    assertEquals("s3a://b", r.getUrl());
  }

  @Test
  void s3RequiresBucket() {
    assertThrows(IllegalArgumentException.class, () -> ObjectStoreOptions.s3().build());
    assertThrows(IllegalArgumentException.class, () -> ObjectStoreOptions.s3().bucket("").build());
  }

  @Test
  void gcsRoundTripsThroughProto() throws Exception {
    byte[] bytes =
        SessionContext.builder()
            .registerObjectStore(
                ObjectStoreOptions.gcs()
                    .bucket("data")
                    .serviceAccountKey("{\"type\":\"service_account\"}")
                    .build())
            .toBytes();
    SessionOptions parsed = SessionOptions.parseFrom(bytes);
    ObjectStoreRegistration r = parsed.getObjectStores(0);
    assertTrue(r.hasGcs());
    assertEquals("data", r.getGcs().getBucket());
    assertEquals("{\"type\":\"service_account\"}", r.getGcs().getServiceAccountKey());
    assertFalse(r.getGcs().hasServiceAccountPath());
  }

  @Test
  void gcsRequiresBucketAndRejectsConflictingCreds() {
    assertThrows(IllegalArgumentException.class, () -> ObjectStoreOptions.gcs().build());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ObjectStoreOptions.gcs()
                .bucket("b")
                .serviceAccountKey("{}")
                .serviceAccountPath("/tmp/sa.json")
                .build());
  }

  @Test
  void httpRoundTripsThroughProto() throws Exception {
    byte[] bytes =
        SessionContext.builder()
            .registerObjectStore(
                ObjectStoreOptions.http("https://example.com/").allowHttp(false).build())
            .toBytes();
    SessionOptions parsed = SessionOptions.parseFrom(bytes);
    ObjectStoreRegistration r = parsed.getObjectStores(0);
    assertTrue(r.hasHttp());
    assertTrue(r.hasUrl());
    assertEquals("https://example.com/", r.getUrl());
    assertFalse(r.getHttp().getAllowHttp());
  }

  @Test
  void httpRequiresUrl() {
    assertThrows(IllegalArgumentException.class, () -> ObjectStoreOptions.http(null));
  }

  @Test
  void multipleRegistrationsPreserveInsertionOrder() throws Exception {
    byte[] bytes =
        SessionContext.builder()
            .registerObjectStore(ObjectStoreOptions.s3().bucket("first").build())
            .registerObjectStore(ObjectStoreOptions.gcs().bucket("second").build())
            .registerObjectStore(ObjectStoreOptions.s3().bucket("third").build())
            .toBytes();
    SessionOptions parsed = SessionOptions.parseFrom(bytes);
    List<ObjectStoreRegistration> regs = parsed.getObjectStoresList();
    assertEquals(3, regs.size());
    assertEquals("first", regs.get(0).getS3().getBucket());
    assertEquals("second", regs.get(1).getGcs().getBucket());
    assertEquals("third", regs.get(2).getS3().getBucket());
  }

  @Test
  void registerObjectStoreRejectsNull() {
    SessionContextBuilder b = SessionContext.builder();
    assertThrows(IllegalArgumentException.class, () -> b.registerObjectStore(null));
  }
}
