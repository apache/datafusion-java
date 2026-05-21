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

import java.util.Locale;

/**
 * Identifies a target OS/arch pair and the path at which the bundled
 * native library is published inside the JAR.
 *
 * <p>Resource layout follows the convention from the
 * {@code datafusion-java} packaging design:
 *
 * <pre>
 *   org/apache/datafusion/linux/amd64/libdatafusion_jni.so
 *   org/apache/datafusion/linux/aarch64/libdatafusion_jni.so
 *   org/apache/datafusion/darwin/x86_64/libdatafusion_jni.dylib
 *   org/apache/datafusion/darwin/aarch64/libdatafusion_jni.dylib
 * </pre>
 *
 * <p>Package-private; consumed only by {@link NativeLibraryLoader}.
 */
final class Platform {

  static final String LIBRARY_NAME = "datafusion_jni";
  static final String RESOURCE_PREFIX = "org/apache/datafusion";

  enum Os {
    LINUX("linux", "lib", "so"),
    DARWIN("darwin", "lib", "dylib"),
    WINDOWS("windows", "", "dll");

    final String dirName;
    final String libPrefix;
    final String libSuffix;

    Os(String dirName, String libPrefix, String libSuffix) {
      this.dirName = dirName;
      this.libPrefix = libPrefix;
      this.libSuffix = libSuffix;
    }
  }

  final Os os;
  final String arch;

  private Platform(Os os, String arch) {
    this.os = os;
    this.arch = arch;
  }

  static Platform current() {
    return of(System.getProperty("os.name"), System.getProperty("os.arch"));
  }

  static Platform of(String osName, String osArch) {
    Os os = detectOs(osName);
    String arch = detectArch(os, osArch);
    return new Platform(os, arch);
  }

  static Os detectOs(String osName) {
    if (osName == null) {
      throw new UnsupportedOperationException("os.name is not set");
    }
    String n = osName.toLowerCase(Locale.ROOT);
    if (n.startsWith("linux")) {
      return Os.LINUX;
    }
    if (n.startsWith("mac") || n.contains("darwin")) {
      return Os.DARWIN;
    }
    if (n.startsWith("windows")) {
      return Os.WINDOWS;
    }
    throw new UnsupportedOperationException(
        "Unsupported OS for datafusion_jni: " + osName);
  }

  /**
   * Returns the architecture segment used in the resource path for {@code os}.
   *
   * <p>Linux uses {@code amd64} (Java's preferred name for x86_64), while
   * macOS uses {@code x86_64}; both use {@code aarch64} for ARM64.
   */
  static String detectArch(Os os, String osArch) {
    if (osArch == null) {
      throw new UnsupportedOperationException("os.arch is not set");
    }
    String n = osArch.toLowerCase(Locale.ROOT);
    boolean isX64 = n.equals("amd64") || n.equals("x86_64") || n.equals("x64");
    boolean isArm64 = n.equals("aarch64") || n.equals("arm64");
    if (isX64) {
      return os == Os.LINUX ? "amd64" : "x86_64";
    }
    if (isArm64) {
      return "aarch64";
    }
    throw new UnsupportedOperationException(
        "Unsupported CPU architecture for datafusion_jni: " + osArch);
  }

  String libFileName() {
    return os.libPrefix + LIBRARY_NAME + "." + os.libSuffix;
  }

  /**
   * Absolute classpath resource path (with leading slash) of the bundled
   * native library for this platform.
   */
  String resourcePath() {
    return "/" + RESOURCE_PREFIX + "/" + os.dirName + "/" + arch + "/" + libFileName();
  }

  @Override
  public String toString() {
    return os.dirName + "/" + arch;
  }
}
