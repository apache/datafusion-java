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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Extracts a cdylib bundled inside a jar to a temp file and loads it via {@link System#load}.
 * Expected layout inside the jar:
 *
 * <pre>
 *   &lt;resourcePrefix&gt;/&lt;os&gt;/&lt;arch&gt;/lib&lt;name&gt;.&lt;ext&gt;
 * </pre>
 *
 * where {@code <os>} is one of {@code linux}, {@code darwin}, {@code windows} and {@code <arch>} is
 * {@code x86_64} or {@code aarch64}.
 *
 * <p>The connector loads its own cdylib through this class (prefix {@code io/datafusion/spark});
 * bridges are encouraged to reuse it via {@link #load(Class, String, String)} from their native
 * class's static initializer, with their own resource prefix, instead of hand-rolling extraction.
 * Bundle the cdylib with the same antrun-copy pattern the connector's pom uses (see "Packaging your
 * bridge" in {@code spark/README.md}).
 */
public final class NativeLibraryLoader {

  /** {@code <resourcePrefix>/<name>} entries already extracted and loaded by this classloader. */
  private static final Set<String> LOADED = ConcurrentHashMap.newKeySet();

  private NativeLibraryLoader() {}

  /** Connector-internal entry: loads from the connector jar's own prefix. */
  static void loadLibrary(String name) {
    load(NativeLibraryLoader.class, "io/datafusion/spark", name);
  }

  /**
   * Extract {@code <resourcePrefix>/<os>/<arch>/<mapped name>} from {@code anchor}'s classloader
   * and {@link System#load} it. Idempotent per (prefix, name): repeated calls — e.g. one per Spark
   * task instantiating the bridge's native class — load once.
   *
   * @param anchor class whose classloader holds the resource (the bridge's own native class, so the
   *     lookup works under Spark's per-application classloaders)
   * @param resourcePrefix jar-internal directory, no leading or trailing slash (e.g. {@code
   *     "com/example/mybridge"})
   * @param name unmapped library name (e.g. {@code "my_bridge"} for {@code libmy_bridge.so})
   * @throws UnsatisfiedLinkError if the resource is missing or extraction fails
   */
  public static void load(Class<?> anchor, String resourcePrefix, String name) {
    String key = resourcePrefix + "/" + name;
    if (!LOADED.add(key)) {
      return;
    }
    String resource =
        String.format(
            "/%s/%s/%s/%s",
            resourcePrefix, currentOs(), currentArch(), System.mapLibraryName(name));
    try (InputStream in = anchor.getResourceAsStream(resource)) {
      if (in == null) {
        LOADED.remove(key);
        throw new UnsatisfiedLinkError("Native library not found on classpath: " + resource);
      }
      Path tmp = Files.createTempFile("libdatafusion-spark-", "-" + System.mapLibraryName(name));
      tmp.toFile().deleteOnExit();
      Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING);
      System.load(tmp.toAbsolutePath().toString());
    } catch (IOException e) {
      LOADED.remove(key);
      throw new UnsatisfiedLinkError(
          "Failed to extract native library " + resource + ": " + e.getMessage());
    } catch (RuntimeException | Error e) {
      LOADED.remove(key);
      throw e;
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
