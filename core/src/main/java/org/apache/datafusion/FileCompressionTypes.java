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

/**
 * Internal helpers for translating between the user-facing {@link FileCompressionType} enum and the
 * protobuf-generated {@code org.apache.datafusion.protobuf.FileCompressionType} enum used on the
 * wire. Same variant set, different Java types.
 */
final class FileCompressionTypes {
  private FileCompressionTypes() {}

  static org.apache.datafusion.protobuf.FileCompressionType toProto(FileCompressionType t) {
    switch (t) {
      case UNCOMPRESSED:
        return org.apache.datafusion.protobuf.FileCompressionType
            .FILE_COMPRESSION_TYPE_UNCOMPRESSED;
      case GZIP:
        return org.apache.datafusion.protobuf.FileCompressionType.FILE_COMPRESSION_TYPE_GZIP;
      case BZIP2:
        return org.apache.datafusion.protobuf.FileCompressionType.FILE_COMPRESSION_TYPE_BZIP2;
      case XZ:
        return org.apache.datafusion.protobuf.FileCompressionType.FILE_COMPRESSION_TYPE_XZ;
      case ZSTD:
        return org.apache.datafusion.protobuf.FileCompressionType.FILE_COMPRESSION_TYPE_ZSTD;
      default:
        throw new IllegalArgumentException("unhandled FileCompressionType: " + t);
    }
  }
}
