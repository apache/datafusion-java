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
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Loads the {@code datafusion_jni} native library on demand.
 *
 * <p>The loader first tries {@link System#loadLibrary(String)} so that
 * operators can override the bundled library by placing a build on
 * {@code java.library.path} (for example via
 * {@code -Djava.library.path=...} or {@code LD_LIBRARY_PATH}). If that
 * fails the loader extracts the platform-specific library from the JAR
 * resource tree and loads it via {@link System#load(String)}.
 *
 * <p>The bundled libraries live at the conventional path
 * {@code org/apache/datafusion/&lt;os&gt;/&lt;arch&gt;/&lt;libfile&gt;}.
 * Extracted files are written under
 * {@code $TMPDIR/datafusion-java/&lt;sha256&gt;/} so that concurrent JVMs
 * sharing a temp directory converge on the same file rather than each
 * extracting their own copy.
 */
public final class NativeLibraryLoader {

  private static final String TMP_DIR_NAME = "datafusion-java";

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
            "No bundled datafusion_jni library for " + platform
                + " (expected classpath:" + resource + ")."
                + " Build the native crate and add it to java.library.path,"
                + " or depend on a JAR built for this platform.");
      }
    } catch (IOException e) {
      throw linkError("Failed to probe " + resource, e);
    }

    try {
      Path extracted = extractToTempDir(resource, platform.libFileName());
      System.load(extracted.toAbsolutePath().toString());
    } catch (IOException e) {
      throw linkError("Failed to extract " + resource, e);
    }
  }

  private static Path extractToTempDir(String resource, String fileName) throws IOException {
    Path tmpRoot = Files.createDirectories(
        Paths.get(System.getProperty("java.io.tmpdir"), TMP_DIR_NAME));
    Path staging = Files.createTempFile(tmpRoot, fileName + ".", ".part");

    String hash;
    try (InputStream raw = NativeLibraryLoader.class.getResourceAsStream(resource);
         DigestInputStream in = new DigestInputStream(raw, sha256());
         OutputStream out = Files.newOutputStream(staging)) {
      in.transferTo(out);
      hash = toHex(in.getMessageDigest().digest());
    } catch (IOException e) {
      Files.deleteIfExists(staging);
      throw e;
    }

    Path versionedDir = Files.createDirectories(tmpRoot.resolve(hash));
    Path target = versionedDir.resolve(fileName);

    if (Files.exists(target) && Files.size(target) == Files.size(staging)) {
      Files.deleteIfExists(staging);
      return target;
    }

    try {
      Files.move(staging, target, StandardCopyOption.ATOMIC_MOVE);
    } catch (FileAlreadyExistsException e) {
      // Another JVM extracted the same content while we were writing.
      // Their copy is identical (same SHA-256), so discard ours.
      Files.deleteIfExists(staging);
    } catch (IOException e) {
      // Atomic move not supported on this filesystem. Fall back to a
      // replacement move; the hash directory guarantees content equality.
      try {
        Files.move(staging, target, StandardCopyOption.REPLACE_EXISTING);
      } catch (IOException retry) {
        Files.deleteIfExists(staging);
        throw retry;
      }
    }
    return target;
  }

  private static MessageDigest sha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }

  private static String toHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      sb.append(Character.forDigit((b >> 4) & 0xf, 16));
      sb.append(Character.forDigit(b & 0xf, 16));
    }
    return sb.toString();
  }

  private static UnsatisfiedLinkError linkError(String message, Throwable cause) {
    UnsatisfiedLinkError err = new UnsatisfiedLinkError(message + ": " + cause.getMessage());
    err.initCause(cause);
    return err;
  }
}
