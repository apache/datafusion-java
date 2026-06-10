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

package org.apache.datafusion.examples;

import java.util.Map;

import io.datafusion.spark.FfiProviderFactory;

/**
 * Minimal {@link FfiProviderFactory} that exposes the example {@code MemTable} produced by {@link
 * FfiTableProviderExampleNative#createMemTableProvider()} as a Spark DataSource V2 source.
 *
 * <p>Wire it into PySpark with:
 *
 * <pre>{@code
 * df = (spark.read.format("datafusion")
 *         .option("df.factory", "org.apache.datafusion.examples.ExampleFfiProviderFactory")
 *         .load())
 * }</pre>
 *
 * <p>No driver-side options are interpreted — the underlying {@code MemTable} is hard-coded in the
 * cdylib at {@code examples/native}. A single partition (id {@code "p0"}) is reported so Spark
 * spawns one task; the executor calls {@link #createProvider(byte[])} to obtain a fresh {@code
 * FFI_TableProvider} pointer, hands it to {@link
 * org.apache.datafusion.SessionContext#registerFfiTable(String, long)}, and streams the resulting
 * Arrow record batches back into the Spark scan.
 */
public final class ExampleFfiProviderFactory implements FfiProviderFactory {

  public ExampleFfiProviderFactory() {}

  @Override
  public byte[] encodeOptions(Map<String, String> sparkOptions) {
    return new byte[0];
  }

  @Override
  public String[] listPartitions(byte[] optionsProtoBytes) {
    return new String[] {"p0"};
  }

  @Override
  public long createProvider(byte[] optionsProtoBytes) {
    return FfiTableProviderExampleNative.createMemTableProvider();
  }
}
