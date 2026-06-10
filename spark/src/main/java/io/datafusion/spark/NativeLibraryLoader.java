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

package io.datafusion.spark;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Locale;

/**
 * Extracts a cdylib bundled inside the connector-core jar to a temp file and loads it via {@link
 * System#load}. Layout inside the jar:
 *
 * <pre>
 *   io/datafusion/spark/&lt;os&gt;/&lt;arch&gt;/lib&lt;name&gt;.&lt;ext&gt;
 * </pre>
 *
 * where {@code <os>} is one of {@code linux}, {@code darwin}, {@code windows} and {@code <arch>} is
 * {@code x86_64} or {@code aarch64}.
 */
final class NativeLibraryLoader {

  private NativeLibraryLoader() {}

  static void loadLibrary(String name) {
    String resource =
        String.format(
            "/io/datafusion/spark/%s/%s/%s",
            currentOs(), currentArch(), System.mapLibraryName(name));
    try (InputStream in = NativeLibraryLoader.class.getResourceAsStream(resource)) {
      if (in == null) {
        throw new UnsatisfiedLinkError("Native library not found on classpath: " + resource);
      }
      Path tmp = Files.createTempFile("libdatafusion-spark-", "-" + System.mapLibraryName(name));
      tmp.toFile().deleteOnExit();
      Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING);
      System.load(tmp.toAbsolutePath().toString());
    } catch (IOException e) {
      throw new UnsatisfiedLinkError(
          "Failed to extract native library " + resource + ": " + e.getMessage());
    }
  }

  private static String currentOs() {
    String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
    if (os.contains("linux")) return "linux";
    if (os.contains("mac") || os.contains("darwin")) return "darwin";
    if (os.contains("windows")) return "windows";
    throw new UnsupportedOperationException("Unsupported OS: " + os);
  }

  private static String currentArch() {
    String arch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);
    if (arch.equals("amd64") || arch.equals("x86_64")) return "x86_64";
    if (arch.equals("aarch64") || arch.equals("arm64")) return "aarch64";
    throw new UnsupportedOperationException("Unsupported arch: " + arch);
  }
}
