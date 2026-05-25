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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Loads the {@code datafusion_jni} native library on demand.
 *
 * <p>The loader first tries {@link System#loadLibrary(String)} so that operators can override the
 * bundled library by placing a build on {@code java.library.path} (for example via {@code
 * -Djava.library.path=...} or {@code LD_LIBRARY_PATH}). If that fails the loader extracts the
 * platform-specific library from the JAR resource tree and loads it via {@link
 * System#load(String)}.
 *
 * <p>The bundled libraries live at the conventional path {@code
 * org/apache/datafusion/&lt;os&gt;/&lt;arch&gt;/&lt;libfile&gt;}. Each JVM extracts into its own
 * private temporary directory created with {@link Files#createTempDirectory(String,
 * java.nio.file.attribute.FileAttribute[])}, which is mode 0700 on POSIX and user-private under the
 * default Windows {@code TMP}. Keeping the directory private prevents another user on the host from
 * planting a same-named library for the JVM to pick up.
 */
public final class NativeLibraryLoader {

  private static final String TMP_DIR_PREFIX = "datafusion-java-";

  private static volatile boolean loaded;

  private NativeLibraryLoader() {}

  public static synchronized void loadLibrary() {
    if (loaded) {
      return;
    }
    if (tryLoadFromLibraryPath()) {
      loaded = true;
      return;
    }
    loadFromClasspath();
    loaded = true;
  }

  private static boolean tryLoadFromLibraryPath() {
    try {
      System.loadLibrary(Platform.LIBRARY_NAME);
      return true;
    } catch (UnsatisfiedLinkError ignored) {
      return false;
    }
  }

  private static void loadFromClasspath() {
    Platform platform = Platform.current();
    String resource = platform.resourcePath();
    try (InputStream check = NativeLibraryLoader.class.getResourceAsStream(resource)) {
      if (check == null) {
        throw new UnsatisfiedLinkError(
            "No bundled datafusion_jni library for "
                + platform
                + " (expected classpath:"
                + resource
                + ")."
                + " Build the native crate and add it to java.library.path,"
                + " or depend on a JAR built for this platform.");
      }
    } catch (IOException e) {
      throw linkError("Failed to probe " + resource, e);
    }

    try {
      Path extracted = extractToPrivateTempDir(resource, platform.libFileName());
      System.load(extracted.toAbsolutePath().toString());
    } catch (IOException e) {
      throw linkError("Failed to extract " + resource, e);
    }
  }

  private static Path extractToPrivateTempDir(String resource, String fileName) throws IOException {
    Path tmpDir = Files.createTempDirectory(TMP_DIR_PREFIX);
    tmpDir.toFile().deleteOnExit();
    Path target = tmpDir.resolve(fileName);
    target.toFile().deleteOnExit();
    try (InputStream in = NativeLibraryLoader.class.getResourceAsStream(resource)) {
      Files.copy(in, target);
    }
    return target;
  }

  private static UnsatisfiedLinkError linkError(String message, Throwable cause) {
    UnsatisfiedLinkError err = new UnsatisfiedLinkError(message + ": " + cause.getMessage());
    err.initCause(cause);
    return err;
  }
}
