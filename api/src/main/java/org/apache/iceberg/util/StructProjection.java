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
package org.apache.iceberg.util;

import java.util.List;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.primitives.Ints;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.StructType;

public class StructProjection implements StructLike {
  /**
   * Creates a projecting wrapper for {@link StructLike} rows.
   *
   * <p>This projection does not work with repeated types like lists and maps.
   *
   * @param schema schema of rows wrapped by this projection
   * @param ids field ids from the row schema to project
   * @return a wrapper to project rows
   */
  public static StructProjection create(Schema schema, Set<Integer> ids) {
    StructType structType = schema.asStruct();
    return new StructProjection(structType, TypeUtil.project(structType, ids));
  }

  /**
   * Creates a projecting wrapper for {@link StructLike} rows.
   *
   * <p>This projection does not work with repeated types like lists and maps.
   *
   * @param dataSchema schema of rows wrapped by this projection
   * @param projectedSchema result schema of the projected rows
   * @return a wrapper to project rows
   */
  public static StructProjection create(Schema dataSchema, Schema projectedSchema) {
    return new StructProjection(dataSchema.asStruct(), projectedSchema.asStruct());
  }

  /**
   * Creates a projecting wrapper for {@link StructLike} rows.
   *
   * <p>This projection does not work with repeated types like lists and maps.
   *
   * @param structType type of rows wrapped by this projection
   * @param projectedStructType result type of the projected rows
   * @return a wrapper to project rows
   */
  public static StructProjection create(StructType structType, StructType projectedStructType) {
    return new StructProjection(structType, projectedStructType);
  }

  /**
   * Creates a projecting wrapper for {@link StructLike} rows.
   *
   * <p>This projection allows missing fields and does not work with repeated types like lists and
   * maps.
   *
   * @param structType type of rows wrapped by this projection
   * @param projectedStructType result type of the projected rows
   * @return a wrapper to project rows
   */
  public static StructProjection createAllowMissing(
      StructType structType, StructType projectedStructType) {
    return new StructProjection(structType, projectedStructType, true);
  }

  private final StructType type;
  private final int[] positionMap;
  private final StructProjection[] nestedProjections;
  private StructLike struct;

  private StructProjection(
      StructType type, int[] positionMap, StructProjection[] nestedProjections) {
    this.type = type;
    this.positionMap = positionMap;
    this.nestedProjections = nestedProjections;
  }

  private StructProjection(StructType structType, StructType projection) {
    this(structType, projection, false);
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private StructProjection(StructType structType, StructType projection, boolean allowMissing) {
    this.type = projection;
    this.positionMap = new int[projection.fields().size()];
    this.nestedProjections = new StructProjection[projection.fields().size()];

    // set up the projection positions and any nested projections that are needed
    List<Types.NestedField> dataFields = structType.fields();
    for (int pos = 0; pos < positionMap.length; pos += 1) {
      Types.NestedField projectedField = projection.fields().get(pos);

      boolean found = false;
      for (int i = 0; !found && i < dataFields.size(); i += 1) {
        Types.NestedField dataField = dataFields.get(i);
        if (projectedField.fieldId() == dataField.fieldId()) {
          found = true;
          positionMap[pos] = i;
          switch (projectedField.type().typeId()) {
            case STRUCT:
              nestedProjections[pos] =
                  new StructProjection(
                      dataField.type().asStructType(), projectedField.type().asStructType());
              break;
            case MAP:
              MapType projectedMap = projectedField.type().asMapType();
              MapType originalMap = dataField.type().asMapType();

              boolean keyProjectable =
                  !projectedMap.keyType().isNestedType()
                      || projectedMap.keyType().equals(originalMap.keyType());
              boolean valueProjectable =
                  !projectedMap.valueType().isNestedType()
                      || projectedMap.valueType().equals(originalMap.valueType());
              Preconditions.checkArgument(
                  keyProjectable && valueProjectable,
                  "Cannot project a partial map key or value struct. Trying to project %s out of %s",
                  projectedField,
                  dataField);

              nestedProjections[pos] = null;
              break;
            case LIST:
              ListType projectedList = projectedField.type().asListType();
              ListType originalList = dataField.type().asListType();

              boolean elementProjectable =
                  !projectedList.elementType().isNestedType()
                      || projectedList.elementType().equals(originalList.elementType());
              Preconditions.checkArgument(
                  elementProjectable,
                  "Cannot project a partial list element struct. Trying to project %s out of %s",
                  projectedField,
                  dataField);

              nestedProjections[pos] = null;
              break;
            default:
              nestedProjections[pos] = null;
          }
        }
      }

      if (!found && projectedField.isOptional() && allowMissing) {
        positionMap[pos] = -1;
        nestedProjections[pos] = null;
      } else if (!found) {
        throw new IllegalArgumentException(
            String.format("Cannot find field %s in %s", projectedField, structType));
      }
    }
  }

  public int projectedFields() {
    return (int) Ints.asList(positionMap).stream().filter(val -> val != -1).count();
  }

  public StructProjection wrap(StructLike newStruct) {
    this.struct = newStruct;
    return this;
  }

  public StructProjection copyFor(StructLike newStruct) {
    return new StructProjection(type, positionMap, nestedProjections).wrap(newStruct);
  }

  @Override
  public int size() {
    return type.fields().size();
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    // struct can be null if wrap is not called first before the get call
    // or if a null struct is wrapped.
    if (struct == null) {
      return null;
    }

    int structPos = positionMap[pos];
    if (nestedProjections[pos] != null) {
      StructLike nestedStruct = struct.get(structPos, StructLike.class);
      if (nestedStruct == null) {
        return null;
      }

      return javaClass.cast(nestedProjections[pos].wrap(nestedStruct));
    }

    if (structPos != -1) {
      return struct.get(structPos, javaClass);
    } else {
      return null;
    }
  }

  @Override
  public <T> void set(int pos, T value) {
    throw new UnsupportedOperationException("Cannot set fields in a TypeProjection");
  }
}
