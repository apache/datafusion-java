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

package org.apache.datafusion.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;

class AdbcOptionsTest {

  private static CaseInsensitiveStringMap opts(Map<String, String> m) {
    return new CaseInsensitiveStringMap(m);
  }

  @Test
  void parsesTargetPartitions() {
    AdbcOptions o =
        AdbcOptions.fromOptions(
            opts(Map.of("driver", "/lib.so", "table", "t", "target_partitions", "16")));
    assertTrue(o.targetPartitions().isPresent());
    assertEquals(16, o.targetPartitions().getAsInt());
  }

  @Test
  void targetPartitionsAbsentByDefault() {
    AdbcOptions o = AdbcOptions.fromOptions(opts(Map.of("driver", "/lib.so", "table", "t")));
    assertFalse(o.targetPartitions().isPresent());
  }

  @Test
  void rejectsNonPositiveOrNonNumeric() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            AdbcOptions.fromOptions(
                opts(Map.of("driver", "/lib.so", "table", "t", "target_partitions", "0"))));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            AdbcOptions.fromOptions(
                opts(Map.of("driver", "/lib.so", "table", "t", "target_partitions", "abc"))));
  }

  @Test
  void controlKeysNotForwardedAsDatabaseOptions() {
    AdbcOptions o =
        AdbcOptions.fromOptions(
            opts(
                Map.of(
                    "driver", "/lib.so",
                    "table", "t",
                    "target_partitions", "8",
                    "my.provider.opt", "v")));
    Map<String, Object> params = o.driverParameters();
    // driver -> jni.driver; provider opt passed through; control keys absent.
    assertTrue(params.containsKey("my.provider.opt"));
    assertFalse(params.containsKey("target_partitions"));
    assertFalse(params.containsKey("table"));
  }
}
