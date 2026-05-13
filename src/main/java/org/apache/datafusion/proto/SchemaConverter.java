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

package org.apache.datafusion.proto;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import datafusion_common.DatafusionCommon;

/**
 * Convert between Arrow Java {@link Schema} and the {@code datafusion_common.Schema} protobuf
 * shape used by DataFusion plan messages such as {@code ListingTableScanNode.schema}.
 *
 * <p>Supports the primitive Arrow types this project's tests exercise (Bool, signed/unsigned Int
 * 8..64, Float32/64, Utf8, Date32, Decimal128). Anything else raises
 * {@link UnsupportedOperationException} with a message naming the offending type.
 */
public final class SchemaConverter {

  private SchemaConverter() {}

  public static DatafusionCommon.Schema toProto(Schema arrow) {
    DatafusionCommon.Schema.Builder builder = DatafusionCommon.Schema.newBuilder();
    for (Field f : arrow.getFields()) {
      builder.addColumns(fieldToProto(f));
    }
    if (arrow.getCustomMetadata() != null) {
      builder.putAllMetadata(arrow.getCustomMetadata());
    }
    return builder.build();
  }

  public static Schema fromProto(DatafusionCommon.Schema proto) {
    List<Field> fields = new ArrayList<>(proto.getColumnsCount());
    for (DatafusionCommon.Field f : proto.getColumnsList()) {
      fields.add(fieldFromProto(f));
    }
    Map<String, String> metadata = new LinkedHashMap<>(proto.getMetadataMap());
    return new Schema(fields, metadata);
  }

  static DatafusionCommon.Field fieldToProto(Field f) {
    DatafusionCommon.Field.Builder b =
        DatafusionCommon.Field.newBuilder()
            .setName(f.getName())
            .setArrowType(arrowTypeToProto(f.getType()))
            .setNullable(f.isNullable());
    if (f.getMetadata() != null) {
      b.putAllMetadata(f.getMetadata());
    }
    return b.build();
  }

  static Field fieldFromProto(DatafusionCommon.Field f) {
    ArrowType type = arrowTypeFromProto(f.getArrowType());
    FieldType ft = new FieldType(f.getNullable(), type, null, f.getMetadataMap());
    return new Field(f.getName(), ft, null);
  }

  static DatafusionCommon.ArrowType arrowTypeToProto(ArrowType t) {
    DatafusionCommon.EmptyMessage empty = DatafusionCommon.EmptyMessage.getDefaultInstance();
    DatafusionCommon.ArrowType.Builder b = DatafusionCommon.ArrowType.newBuilder();

    if (t instanceof ArrowType.Bool) {
      return b.setBOOL(empty).build();
    }
    if (t instanceof ArrowType.Int) {
      ArrowType.Int i = (ArrowType.Int) t;
      if (i.getIsSigned()) {
        switch (i.getBitWidth()) {
          case 8: return b.setINT8(empty).build();
          case 16: return b.setINT16(empty).build();
          case 32: return b.setINT32(empty).build();
          case 64: return b.setINT64(empty).build();
          default:
            throw new UnsupportedOperationException(
                "Arrow type Int signed width " + i.getBitWidth() + " not yet supported by SchemaConverter");
        }
      } else {
        switch (i.getBitWidth()) {
          case 8: return b.setUINT8(empty).build();
          case 16: return b.setUINT16(empty).build();
          case 32: return b.setUINT32(empty).build();
          case 64: return b.setUINT64(empty).build();
          default:
            throw new UnsupportedOperationException(
                "Arrow type Int unsigned width " + i.getBitWidth() + " not yet supported by SchemaConverter");
        }
      }
    }
    if (t instanceof ArrowType.FloatingPoint) {
      FloatingPointPrecision p = ((ArrowType.FloatingPoint) t).getPrecision();
      if (p == FloatingPointPrecision.SINGLE) return b.setFLOAT32(empty).build();
      if (p == FloatingPointPrecision.DOUBLE) return b.setFLOAT64(empty).build();
      throw new UnsupportedOperationException(
          "Arrow type FloatingPoint " + p + " not yet supported by SchemaConverter");
    }
    if (t instanceof ArrowType.Utf8) {
      return b.setUTF8(empty).build();
    }
    if (t instanceof ArrowType.Date) {
      DateUnit u = ((ArrowType.Date) t).getUnit();
      if (u == DateUnit.DAY) return b.setDATE32(empty).build();
      throw new UnsupportedOperationException(
          "Arrow type Date " + u + " not yet supported by SchemaConverter");
    }
    if (t instanceof ArrowType.Decimal) {
      ArrowType.Decimal d = (ArrowType.Decimal) t;
      if (d.getBitWidth() != 128) {
        throw new UnsupportedOperationException(
            "Arrow type Decimal bit width " + d.getBitWidth() + " not yet supported by SchemaConverter");
      }
      return b.setDECIMAL128(
              DatafusionCommon.Decimal128Type.newBuilder()
                  .setPrecision(d.getPrecision())
                  .setScale(d.getScale())
                  .build())
          .build();
    }
    throw new UnsupportedOperationException(
        "Arrow type " + t.getClass().getSimpleName() + " not yet supported by SchemaConverter");
  }

  static ArrowType arrowTypeFromProto(DatafusionCommon.ArrowType p) {
    switch (p.getArrowTypeEnumCase()) {
      case BOOL:
        return ArrowType.Bool.INSTANCE;
      case INT8: return new ArrowType.Int(8, true);
      case INT16: return new ArrowType.Int(16, true);
      case INT32: return new ArrowType.Int(32, true);
      case INT64: return new ArrowType.Int(64, true);
      case UINT8: return new ArrowType.Int(8, false);
      case UINT16: return new ArrowType.Int(16, false);
      case UINT32: return new ArrowType.Int(32, false);
      case UINT64: return new ArrowType.Int(64, false);
      case FLOAT32: return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
      case FLOAT64: return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
      case UTF8: return ArrowType.Utf8.INSTANCE;
      case DATE32: return new ArrowType.Date(DateUnit.DAY);
      case DECIMAL128:
        DatafusionCommon.Decimal128Type d = p.getDECIMAL128();
        return new ArrowType.Decimal(d.getPrecision(), d.getScale(), 128);
      default:
        throw new UnsupportedOperationException(
            "datafusion_common.ArrowType " + p.getArrowTypeEnumCase() + " not yet supported by SchemaConverter");
    }
  }
}
