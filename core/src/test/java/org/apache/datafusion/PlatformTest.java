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
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class PlatformTest {

  @Test
  void detectsLinuxFromOsName() {
    assertEquals(Platform.Os.LINUX, Platform.detectOs("Linux"));
    assertEquals(Platform.Os.LINUX, Platform.detectOs("linux"));
  }

  @Test
  void detectsDarwinFromOsName() {
    assertEquals(Platform.Os.DARWIN, Platform.detectOs("Mac OS X"));
    assertEquals(Platform.Os.DARWIN, Platform.detectOs("macOS"));
    assertEquals(Platform.Os.DARWIN, Platform.detectOs("Darwin"));
  }

  @Test
  void detectsWindowsFromOsName() {
    assertEquals(Platform.Os.WINDOWS, Platform.detectOs("Windows 10"));
    assertEquals(Platform.Os.WINDOWS, Platform.detectOs("Windows Server 2019"));
  }

  @Test
  void rejectsUnknownOs() {
    assertThrows(UnsupportedOperationException.class, () -> Platform.detectOs("Solaris"));
  }

  @Test
  void rejectsNullOs() {
    assertThrows(UnsupportedOperationException.class, () -> Platform.detectOs(null));
  }

  @Test
  void usesAmd64OnLinuxForX86_64Aliases() {
    assertEquals("amd64", Platform.detectArch(Platform.Os.LINUX, "amd64"));
    assertEquals("amd64", Platform.detectArch(Platform.Os.LINUX, "x86_64"));
    assertEquals("amd64", Platform.detectArch(Platform.Os.LINUX, "x64"));
  }

  @Test
  void usesX86_64OnDarwinForX86_64Aliases() {
    assertEquals("x86_64", Platform.detectArch(Platform.Os.DARWIN, "amd64"));
    assertEquals("x86_64", Platform.detectArch(Platform.Os.DARWIN, "x86_64"));
  }

  @Test
  void usesAmd64OnWindowsForX86_64Aliases() {
    assertEquals("amd64", Platform.detectArch(Platform.Os.WINDOWS, "amd64"));
    assertEquals("amd64", Platform.detectArch(Platform.Os.WINDOWS, "x86_64"));
    assertEquals("amd64", Platform.detectArch(Platform.Os.WINDOWS, "x64"));
  }

  @Test
  void usesAarch64ForArm64Aliases() {
    assertEquals("aarch64", Platform.detectArch(Platform.Os.LINUX, "aarch64"));
    assertEquals("aarch64", Platform.detectArch(Platform.Os.LINUX, "arm64"));
    assertEquals("aarch64", Platform.detectArch(Platform.Os.DARWIN, "aarch64"));
    assertEquals("aarch64", Platform.detectArch(Platform.Os.DARWIN, "arm64"));
  }

  @Test
  void rejectsUnknownArch() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> Platform.detectArch(Platform.Os.LINUX, "ppc64le"));
  }

  @Test
  void libFileNameUsesPlatformConventions() {
    assertEquals("libdatafusion_jni.so", Platform.of("Linux", "amd64").libFileName());
    assertEquals("libdatafusion_jni.so", Platform.of("Linux", "aarch64").libFileName());
    assertEquals("libdatafusion_jni.dylib", Platform.of("Mac OS X", "x86_64").libFileName());
    assertEquals("libdatafusion_jni.dylib", Platform.of("Mac OS X", "aarch64").libFileName());
    assertEquals("datafusion_jni.dll", Platform.of("Windows 11", "amd64").libFileName());
  }

  @Test
  void resourcePathMatchesSpec() {
    assertEquals(
        "/org/apache/datafusion/linux/amd64/libdatafusion_jni.so",
        Platform.of("Linux", "amd64").resourcePath());
    assertEquals(
        "/org/apache/datafusion/linux/aarch64/libdatafusion_jni.so",
        Platform.of("Linux", "aarch64").resourcePath());
    assertEquals(
        "/org/apache/datafusion/darwin/x86_64/libdatafusion_jni.dylib",
        Platform.of("Mac OS X", "x86_64").resourcePath());
    assertEquals(
        "/org/apache/datafusion/darwin/aarch64/libdatafusion_jni.dylib",
        Platform.of("Mac OS X", "aarch64").resourcePath());
    assertEquals(
        "/org/apache/datafusion/windows/amd64/datafusion_jni.dll",
        Platform.of("Windows 11", "amd64").resourcePath());
  }
}
