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
// import org.slf4j.LoggerFactory;
// import org.slf4j.Logger;

public class BetweenColumnTransformer extends CompareTernaryColumnTransformer {
  private final boolean isNotBetween;
  // private final static Logger logger = LoggerFactory.getLogger(BetweenColumnTransformer.class);

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
      // System.out.println("chose doTransformRCC");
      return;
    } else if (firstColumn instanceof RunLengthEncodedColumn && secondColumn instanceof RLEColumn && thirdColumn instanceof RunLengthEncodedColumn) {
      doTransformCRC(firstColumn, secondColumn, thirdColumn,builder, positionCount);
      return;
    } else if (firstColumn instanceof RunLengthEncodedColumn && secondColumn instanceof RunLengthEncodedColumn && thirdColumn instanceof RLEColumn) {
      doTransformCCR(firstColumn, secondColumn, thirdColumn, builder, positionCount);
    } else if (firstColumn instanceof RLEColumn && secondColumn instanceof RunLengthEncodedColumn && thirdColumn instanceof RLEColumn) {
      doTransformRCR(firstColumn, secondColumn, thirdColumn, builder, positionCount);
    }
    doTransformElse(firstColumn, secondColumn, thirdColumn, builder, positionCount);
    // System.out.println("chose doTransformElse");
  }

  private void doTransformRCR(
    Column firstColumn,
    Column secondColumn,
    Column thirdColumn,
    ColumnBuilder builder,
    int positionCount) {
      Pair<Column[], int[]> leftPatterns = ((RLEColumn) firstColumn).getVisibleColumns();
      Pair<Column[], int[]> rightPatterns = ((RLEColumn) thirdColumn).getVisibleColumns();
      int leftPatternCount = leftPatterns.getLeft().length;
      int rightPatternCount = rightPatterns.getLeft().length;
      int leftIndex = 0, rightIndex = 0;
      int curLeft = 0, curRight = 0;
      int curLeftPositionCount = 0, curRightPositionCount = 0;
      Column leftPatternColumn = leftPatterns.getLeft()[0];
      Column rightPatternColumn = rightPatterns.getLeft()[0];
      boolean isRLELeft = true, isRLERight = true;
      int index = 0;
      int length = 0;

      if (secondColumn.isNull(0)) {
        ColumnBuilder columnBuilderTmp = returnType.createColumnBuilder(1);
        columnBuilderTmp.appendNull();
        ((RLEColumnBuilder) builder).writeRLEPattern(columnBuilderTmp.build(), positionCount);
        return;
      }
  
      while (index < positionCount) {
        if (curLeft == curLeftPositionCount) {
          if (leftIndex < leftPatternCount) {
            curLeft = 0;
            leftPatternColumn = leftPatterns.getLeft()[leftIndex];
            curLeftPositionCount = leftPatterns.getRight()[leftIndex];
            isRLELeft = leftPatternColumn.getPositionCount() == 1;
            leftIndex++;
          } else {
            if (rightIndex < rightPatternCount - 1) {
              throw new RuntimeException("leftColumn and rightColumn have unequal length");
            } else {
              break;
            }
          }
        }
  
        if (curRight == curRightPositionCount) {
          if (rightIndex < rightPatternCount) {
            curRight = 0;
            rightPatternColumn = rightPatterns.getLeft()[rightIndex];
            curRightPositionCount = rightPatterns.getRight()[rightIndex];
            isRLERight = rightPatternColumn.getPositionCount() == 1;
            rightIndex++;
          } else {
            if (leftIndex < leftPatternCount - 1) {
              throw new RuntimeException("leftColumn and rightColumn have unequal length");
            } else {
              break;
            }
          }
        }
        length =
            curLeftPositionCount - curLeft > curRightPositionCount - curRight
                ? curRightPositionCount - curRight
                : curLeftPositionCount - curLeft;
        length = length > positionCount - index ? positionCount - index : length;

        if (isRLELeft && isRLERight) {
          ColumnBuilder columnBuilderTmp = returnType.createColumnBuilder(1);
          boolean flag = false;
          if (!leftPatternColumn.isNull(0) && !rightPatternColumn.isNull(0)) {
            if (firstColumnTransformer.getType() instanceof BinaryType) {
              flag = 
                  ((TransformUtils.compare(
                             firstColumnTransformer.getType().getBinary(leftPatternColumn, 0),
                             secondColumnTransformer.getType().getBinary(secondColumn, 0))
                              >= 0)
                          && (TransformUtils.compare(
                              firstColumnTransformer.getType().getBinary(leftPatternColumn, 0),
                              thirdColumnTransformer.getType().getBinary(rightPatternColumn, 0))
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
                                  thirdColumnTransformer.getType().getDouble(rightPatternColumn, 0))
                                  <= 0))
                      ^ isNotBetween;
            }
            returnType.writeBoolean(columnBuilderTmp, flag);
          } else {
            columnBuilderTmp.appendNull();
          }
          ((RLEColumnBuilder) builder).writeRLEPattern(columnBuilderTmp.build(), length);
          index += length;
          curRight += length;
          curLeft += length;
        } else if (isRLELeft) {
          ColumnBuilder columnBuilderTmp = returnType.createColumnBuilder(1);
          boolean flag = false;
          if (!leftPatternColumn.isNull(0)) {
            if (firstColumnTransformer.getType() instanceof BinaryType) {
              for (int i = 0; i < length; i++, curRight++, index++) {
                if (!rightPatternColumn.isNull(curRight)) {
                  flag = 
                      ((TransformUtils.compare(
                                firstColumnTransformer.getType().getBinary(leftPatternColumn, 0),
                                secondColumnTransformer.getType().getBinary(secondColumn, 0))
                                  >= 0)
                              && (TransformUtils.compare(
                                  firstColumnTransformer.getType().getBinary(leftPatternColumn, 0),
                                  thirdColumnTransformer.getType().getBinary(rightPatternColumn, curRight))
                                      <= 0))
                  ^ isNotBetween;
                } else {
                  columnBuilderTmp.appendNull();
                }
                returnType.writeBoolean(columnBuilderTmp, flag);
              }
            } else {
              for (int i = 0; i < length; i++, curRight++, index++) {
                if (!rightPatternColumn.isNull(curRight)) {
                  flag = 
                      ((Double.compare(
                                    firstColumnTransformer.getType().getDouble(leftPatternColumn, 0),
                                    secondColumnTransformer.getType().getDouble(secondColumn, 0))
                                  >= 0)
                              && (Double.compare(
                                      firstColumnTransformer.getType().getDouble(leftPatternColumn, 0),
                                      thirdColumnTransformer.getType().getDouble(rightPatternColumn, curRight))
                                      <= 0))
                          ^ isNotBetween;
                } else {
                  columnBuilderTmp.appendNull();
                }
                returnType.writeBoolean(columnBuilderTmp, flag);
              }
            }
          } else {
            columnBuilderTmp.appendNull();
          }
          ((RLEColumnBuilder) builder).writeRLEPattern(columnBuilderTmp.build(), length);
          curLeft += length;
        } else if (isRLERight) {
          ColumnBuilder columnBuilderTmp = returnType.createColumnBuilder(1);
          boolean flag = false;
          if (!rightPatternColumn.isNull(0)) {
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
                                  thirdColumnTransformer.getType().getBinary(rightPatternColumn, 0))
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
                                      thirdColumnTransformer.getType().getDouble(rightPatternColumn, 0))
                                      <= 0))
                          ^ isNotBetween;
                } else {
                  columnBuilderTmp.appendNull();
                }
                returnType.writeBoolean(columnBuilderTmp, flag);
              }
            }
          } else {
            columnBuilderTmp.appendNull();
          }
          ((RLEColumnBuilder) builder).writeRLEPattern(columnBuilderTmp.build(), length);
          curRight += length;
        } else {
          ColumnBuilder columnBuilderTmp = returnType.createColumnBuilder(1);
          boolean flag = false;
          for (int i = 0; i < length; i++, curLeft++, curRight++, index++) {
            if (!leftPatternColumn.isNull(curLeft) && !rightPatternColumn.isNull(curRight)) {
              if (firstColumnTransformer.getType() instanceof BinaryType) {
                flag = 
                    ((TransformUtils.compare(
                              firstColumnTransformer.getType().getBinary(leftPatternColumn, curLeft),
                              secondColumnTransformer.getType().getBinary(secondColumn, 0))
                                >= 0)
                            && (TransformUtils.compare(
                                firstColumnTransformer.getType().getBinary(leftPatternColumn, curLeft),
                                thirdColumnTransformer.getType().getBinary(rightPatternColumn, 0))
                                    <= 0))
                        ^ isNotBetween;
              } else {
                flag = 
                    ((Double.compare(
                                  firstColumnTransformer.getType().getDouble(leftPatternColumn, curLeft),
                                  secondColumnTransformer.getType().getDouble(secondColumn, 0))
                                >= 0)
                            && (Double.compare(
                                    firstColumnTransformer.getType().getDouble(leftPatternColumn, curLeft),
                                    thirdColumnTransformer.getType().getDouble(rightPatternColumn, 0))
                                    <= 0))
                        ^ isNotBetween;
              }
              returnType.writeBoolean(columnBuilderTmp, flag);
            } else {
              columnBuilderTmp.appendNull();
            }
          }
          ((RLEColumnBuilder) builder).writeRLEPattern(columnBuilderTmp.build(), length);
        }
      }
    }

  private void doTransformCCR(
    Column firstColumn,
    Column secondColumn,
    Column thirdColumn,
    ColumnBuilder builder,
    int positionCount) {
      Pair<Column[], int[]> rightPatterns = ((RLEColumn) thirdColumn).getVisibleColumns();
      int rightPatternsCount = rightPatterns.getLeft().length;
      int rightIndex = 0, curRight = 0, curRightPositionCount = 0;
      Column rightPatternColumn = rightPatterns.getLeft()[0];
      int index = 0;
      int length = 0;

      if (firstColumn.isNull(0) || secondColumn.isNull(0)) {
        ColumnBuilder columnBuilderTmp = returnType.createColumnBuilder(1);
        columnBuilderTmp.appendNull();
        ((RLEColumnBuilder) builder).writeRLEPattern(columnBuilderTmp.build(), positionCount);
        return;
      }

      while (index < positionCount) {
        if (curRight == curRightPositionCount) {
          if (rightIndex + 1 < rightPatternsCount) {
            curRight = 0;
            rightPatternColumn = rightPatterns.getLeft()[rightIndex];
            curRightPositionCount = rightPatterns.getRight()[rightIndex];
            rightIndex++;
          } else {
            throw new RuntimeException(
                "The positionCount of rightColumn is less than the requested positionCount");
          }
        }

        length = 
            curRightPositionCount - curRight > positionCount - index
            ? positionCount - index
            : curRightPositionCount - curRight;
        if (rightPatternColumn.getPositionCount() == 1) {
          ColumnBuilder columnBuilderTmp = returnType.createColumnBuilder(1);
          boolean flag = false;
          if (!rightPatternColumn.isNull(0)) {
            if (firstColumnTransformer.getType() instanceof BinaryType) {
              flag = 
                  ((TransformUtils.compare(
                                 firstColumnTransformer.getType().getBinary(firstColumn, 0),
                                 secondColumnTransformer.getType().getBinary(secondColumn, 0))
                              >= 0)
                          && (TransformUtils.compare(
                                  firstColumnTransformer.getType().getBinary(firstColumn, 0),
                                  thirdColumnTransformer.getType().getBinary(rightPatternColumn, 0))
                                  <= 0))
                      ^ isNotBetween;
            } else {
              flag = 
                  ((Double.compare(
                                 firstColumnTransformer.getType().getDouble(firstColumn, 0),
                                 secondColumnTransformer.getType().getDouble(secondColumn, 0))
                              >= 0)
                          && (Double.compare(
                                  firstColumnTransformer.getType().getDouble(firstColumn, 0),
                                  thirdColumnTransformer.getType().getDouble(rightPatternColumn, 0))
                                  <= 0))
                      ^ isNotBetween;
              }
              returnType.writeBoolean(columnBuilderTmp, flag);
          } else {
            columnBuilderTmp.appendNull();
          }
          ((RLEColumnBuilder) builder).writeRLEPattern(columnBuilderTmp.build(), length);
          index += length;
          curRight += length;
        } else {
          ColumnBuilder columnBuilderTmp = returnType.createColumnBuilder(1);
          boolean flag = false;
          if (firstColumnTransformer.getType() instanceof BinaryType) {
            for (int i = 0; i < length; i++, curRight++, index++) {
              if (!rightPatternColumn.isNull(curRight)) {
                flag = 
                ((TransformUtils.compare(
                               firstColumnTransformer.getType().getBinary(firstColumn, 0),
                               secondColumnTransformer.getType().getBinary(secondColumn, 0))
                            >= 0)
                        && (TransformUtils.compare(
                                firstColumnTransformer.getType().getBinary(firstColumn, 0),
                                thirdColumnTransformer.getType().getBinary(rightPatternColumn, curRight))
                                <= 0))
                    ^ isNotBetween;
              } else {
                columnBuilderTmp.appendNull();
              }
              returnType.writeBoolean(columnBuilderTmp, flag);
            }
          } else {
            for (int i = 0; i < length; i++, curRight++, index++) {
              if (!rightPatternColumn.isNull(curRight)) {
                flag = 
                ((Double.compare(
                               firstColumnTransformer.getType().getDouble(firstColumn, 0),
                               secondColumnTransformer.getType().getDouble(secondColumn, 0))
                            >= 0)
                        && (Double.compare(
                                firstColumnTransformer.getType().getDouble(secondColumn, 0),
                                thirdColumnTransformer.getType().getDouble(rightPatternColumn, curRight))
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

  private void doTransformCRC(
    Column firstColumn,
    Column secondColumn,
    Column thirdColumn,
    ColumnBuilder builder,
    int positionCount) {
      Pair<Column[], int[]> midPatterns = ((RLEColumn) secondColumn).getVisibleColumns();
      int midPatternsCount = midPatterns.getLeft().length;
      int midIndex = 0, curMid = 0, curMidPositionCount = 0;
      Column midPatternColumn = midPatterns.getLeft()[0];
      int index = 0;
      int length = 0;

      if (firstColumn.isNull(0) || thirdColumn.isNull(0)) {
        ColumnBuilder columnBuilderTmp = returnType.createColumnBuilder(1);
        columnBuilderTmp.appendNull();
        ((RLEColumnBuilder) builder).writeRLEPattern(columnBuilderTmp.build(), positionCount);
        return;
      }

      while (index < positionCount) {
        if (curMid == curMidPositionCount) {
          if (midIndex + 1 < midPatternsCount) {
            curMid = 0;
            midPatternColumn = midPatterns.getLeft()[midIndex];
            curMidPositionCount = midPatterns.getRight()[midIndex];
            midIndex++;
          } else {
            throw new RuntimeException(
              "The positionCount of rightColumn is less than the requested positionCount");
          }
        }

        length = 
            curMidPositionCount - curMid > positionCount - index
            ? positionCount - index
            : curMidPositionCount - curMid;
        if (midPatternColumn.getPositionCount() == 1) {
          ColumnBuilder columnBuilderTmp = returnType.createColumnBuilder(1);
          boolean flag = false;
          if (!midPatternColumn.isNull(0)) {
            if (firstColumnTransformer.getType() instanceof BinaryType) {
              flag = 
                  ((TransformUtils.compare(
                                 firstColumnTransformer.getType().getBinary(firstColumn, 0),
                                 secondColumnTransformer.getType().getBinary(midPatternColumn, 0))
                              >= 0)
                          && (TransformUtils.compare(
                                  firstColumnTransformer.getType().getBinary(firstColumn, 0),
                                  thirdColumnTransformer.getType().getBinary(thirdColumn, 0))
                                  <= 0))
                      ^ isNotBetween;
            } else {
              flag = 
                  ((Double.compare(
                                 firstColumnTransformer.getType().getDouble(firstColumn, 0),
                                 secondColumnTransformer.getType().getDouble(midPatternColumn, 0))
                              >= 0)
                          && (Double.compare(
                                  firstColumnTransformer.getType().getDouble(firstColumn, 0),
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
          curMid += length;
        } else {
          ColumnBuilder columnBuilderTmp = returnType.createColumnBuilder(1);
          boolean flag = false;
          if (firstColumnTransformer.getType() instanceof BinaryType) {
            for (int i = 0; i < length; i++, curMid++, index++) {
              if (!midPatternColumn.isNull(curMid)) {
                flag = 
                ((TransformUtils.compare(
                               firstColumnTransformer.getType().getBinary(firstColumn, 0),
                               secondColumnTransformer.getType().getBinary(midPatternColumn, curMid))
                            >= 0)
                        && (TransformUtils.compare(
                                firstColumnTransformer.getType().getBinary(firstColumn, 0),
                                thirdColumnTransformer.getType().getBinary(thirdColumn, 0))
                                <= 0))
                    ^ isNotBetween;
              } else {
                columnBuilderTmp.appendNull();
              }
              returnType.writeBoolean(columnBuilderTmp, flag);
            }
          } else {
            for (int i = 0; i < length; i++, curMid++, index++) {
              if (!midPatternColumn.isNull(curMid)) {
                flag = 
                ((Double.compare(
                               firstColumnTransformer.getType().getDouble(firstColumn, 0),
                               secondColumnTransformer.getType().getDouble(midPatternColumn, curMid))
                            >= 0)
                        && (Double.compare(
                                firstColumnTransformer.getType().getDouble(secondColumn, 0),
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

      if (secondColumn.isNull(0) || thirdColumn.isNull(0)) {
        ColumnBuilder columnBuilderTmp = returnType.createColumnBuilder(1);
        columnBuilderTmp.appendNull();
        ((RLEColumnBuilder) builder).writeRLEPattern(columnBuilderTmp.build(), positionCount);
        return;
      }

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
