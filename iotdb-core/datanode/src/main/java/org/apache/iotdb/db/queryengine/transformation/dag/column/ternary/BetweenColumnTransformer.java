/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.transformation.dag.column.ternary;

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.util.TransformUtils;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.RLEColumn;
import org.apache.iotdb.tsfile.read.common.block.column.RLEColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.iotdb.tsfile.read.common.type.BinaryType;
import org.apache.iotdb.tsfile.read.common.type.Type;
import org.apache.iotdb.tsfile.utils.Pair;

public class BetweenColumnTransformer extends CompareTernaryColumnTransformer {
  private final boolean isNotBetween;

  public BetweenColumnTransformer(
      Type returnType,
      ColumnTransformer firstColumnTransformer,
      ColumnTransformer secondColumnTransformer,
      ColumnTransformer thirdColumnTransformer,
      boolean isNotBetween) {
    super(returnType, firstColumnTransformer, secondColumnTransformer, thirdColumnTransformer);
    this.isNotBetween = isNotBetween;
  }

  @Override
  protected void doTransform(
    Column firstColumn,
    Column secondColumn,
    Column thirdColumn,
    ColumnBuilder builder,
    int positionCount
  ) {
    if (firstColumn instanceof RLEColumn && secondColumn instanceof RunLengthEncodedColumn && thirdColumn instanceof RunLengthEncodedColumn) {
      doTransformRCC(firstColumn, secondColumn, thirdColumn, builder, positionCount);
      return;
    }
    doTransformElse(firstColumn, secondColumn, thirdColumn, builder, positionCount);
  }

  private void doTransformRCC(
    Column firstColumn,
    Column secondColumn,
    Column thirdColumn,
    ColumnBuilder builder,
    int positionCount) {
      Pair<Column[], int[]> leftPatterns = ((RLEColumn) firstColumn).getVisibleColumns();
      int leftPatternsCount = leftPatterns.getLeft().length;
      int leftIndex = 0, curLeft = 0, curLeftPositionCount = 0;
      Column leftPatternColumn = leftPatterns.getLeft()[0];
      int index = 0;
      int length = 0;

      while (index < positionCount) {
        if (curLeft == curLeftPositionCount) {
          if (leftIndex + 1 < leftPatternsCount) {
            curLeft = 0;
            leftPatternColumn = leftPatterns.getLeft()[leftIndex];
            curLeftPositionCount = leftPatterns.getRight()[leftIndex];
            leftIndex++;
          } else {
            throw new RuntimeException(
              "The positionCount of rightColumn is less than the requested positionCount");
          }
        }

        length = 
            curLeftPositionCount - curLeft > positionCount - index
            ? positionCount - index
            : curLeftPositionCount - curLeft;
        if (leftPatternColumn.getPositionCount() == 1) {
          ColumnBuilder columnBuilderTmp = returnType.createColumnBuilder(1);
          boolean flag = false;
          if (!leftPatternColumn.isNull(0)) {
            if (firstColumnTransformer.getType() instanceof BinaryType) {
              flag = 
                  ((TransformUtils.compare(
                                 firstColumnTransformer.getType().getBinary(leftPatternColumn, 0),
                                 secondColumnTransformer.getType().getBinary(secondColumn, 0))
                              >= 0)
                          && (TransformUtils.compare(
                                  firstColumnTransformer.getType().getBinary(leftPatternColumn, 0),
                                  thirdColumnTransformer.getType().getBinary(thirdColumn, 0))
                                  <= 0))
                      ^ isNotBetween;
            } else {
              flag = 
                  ((Double.compare(
                                 firstColumnTransformer.getType().getDouble(leftPatternColumn, 0),
                                 secondColumnTransformer.getType().getDouble(secondColumn, 0))
                              >= 0)
                          && (Double.compare(
                                  firstColumnTransformer.getType().getDouble(leftPatternColumn, 0),
                                  thirdColumnTransformer.getType().getDouble(thirdColumn, 0))
                                  <= 0))
                      ^ isNotBetween;
              }
              returnType.writeBoolean(columnBuilderTmp, flag);
          } else {
            columnBuilderTmp.appendNull();
          }
          ((RLEColumnBuilder) builder).writeRLEPattern(columnBuilderTmp.build(), length);
          index += length;
          curLeft += length;
        } else {
          ColumnBuilder columnBuilderTmp = returnType.createColumnBuilder(1);
          boolean flag = false;
          if (firstColumnTransformer.getType() instanceof BinaryType) {
            for (int i = 0; i < length; i++, curLeft++, index++) {
              if (!leftPatternColumn.isNull(curLeft)) {
                flag = 
                ((TransformUtils.compare(
                               firstColumnTransformer.getType().getBinary(leftPatternColumn, curLeft),
                               secondColumnTransformer.getType().getBinary(secondColumn, 0))
                            >= 0)
                        && (TransformUtils.compare(
                                firstColumnTransformer.getType().getBinary(leftPatternColumn, curLeft),
                                thirdColumnTransformer.getType().getBinary(thirdColumn, 0))
                                <= 0))
                    ^ isNotBetween;
              } else {
                columnBuilderTmp.appendNull();
              }
              returnType.writeBoolean(columnBuilderTmp, flag);
            }
          } else {
            for (int i = 0; i < length; i++, curLeft++, index++) {
              if (!leftPatternColumn.isNull(curLeft)) {
                flag = 
                ((Double.compare(
                               firstColumnTransformer.getType().getDouble(leftPatternColumn, curLeft),
                               secondColumnTransformer.getType().getDouble(secondColumn, 0))
                            >= 0)
                        && (Double.compare(
                                firstColumnTransformer.getType().getDouble(leftPatternColumn, curLeft),
                                thirdColumnTransformer.getType().getDouble(thirdColumn, 0))
                                <= 0))
                    ^ isNotBetween;
              } else {
                columnBuilderTmp.appendNull();
              }
              returnType.writeBoolean(columnBuilderTmp, flag);
            }
          }
          ((RLEColumnBuilder) builder).writeRLEPattern(columnBuilderTmp.build(), length);
        }
      }
    }

  private void doTransformElse(
      Column firstColumn,
      Column secondColumn,
      Column thirdColumn,
      ColumnBuilder builder,
      int positionCount) {
    for (int i = 0; i < positionCount; i++) {
      if (!firstColumn.isNull(i) && !secondColumn.isNull(i) && !thirdColumn.isNull(i)) {
        boolean flag;
        if (firstColumnTransformer.getType() instanceof BinaryType) {
          flag =
              ((TransformUtils.compare(
                              firstColumnTransformer.getType().getBinary(firstColumn, i),
                              secondColumnTransformer.getType().getBinary(secondColumn, i))
                          >= 0)
                      && (TransformUtils.compare(
                              firstColumnTransformer.getType().getBinary(firstColumn, i),
                              thirdColumnTransformer.getType().getBinary(thirdColumn, i))
                          <= 0))
                  ^ isNotBetween;
        } else {
          flag =
              ((Double.compare(
                              firstColumnTransformer.getType().getDouble(firstColumn, i),
                              secondColumnTransformer.getType().getDouble(secondColumn, i))
                          >= 0)
                      && (Double.compare(
                              firstColumnTransformer.getType().getDouble(firstColumn, i),
                              thirdColumnTransformer.getType().getDouble(thirdColumn, i))
                          <= 0))
                  ^ isNotBetween;
        }
        returnType.writeBoolean(builder, flag);
      } else {
        builder.appendNull();
      }
    }
  }
}
