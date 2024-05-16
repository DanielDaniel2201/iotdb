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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BetweenColumnTransformer extends CompareTernaryColumnTransformer {
  private final boolean isNotBetween;
  private static final Logger logger = LoggerFactory.getLogger(BetweenColumnTransformer.class);

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
      int positionCount) {
    logger.info("pass through modified doTransform");
    if (firstColumn instanceof RLEColumn
        && secondColumn instanceof RunLengthEncodedColumn
        && thirdColumn instanceof RunLengthEncodedColumn) {
      doTransformRCC(firstColumn, secondColumn, thirdColumn, builder, positionCount);
      return;
    } else if (firstColumn instanceof RunLengthEncodedColumn
        && secondColumn instanceof RLEColumn
        && thirdColumn instanceof RunLengthEncodedColumn) {
      doTransformCRC(firstColumn, secondColumn, thirdColumn, builder, positionCount);
      return;
    } else if (firstColumn instanceof RunLengthEncodedColumn
        && secondColumn instanceof RunLengthEncodedColumn
        && thirdColumn instanceof RLEColumn) {
      doTransformCCR(firstColumn, secondColumn, thirdColumn, builder, positionCount);
      return;
    } else if (firstColumn instanceof RLEColumn
        && secondColumn instanceof RunLengthEncodedColumn
        && thirdColumn instanceof RLEColumn) {
      doTransformRCR(firstColumn, secondColumn, thirdColumn, builder, positionCount);
      return;
    } else if (firstColumn instanceof RunLengthEncodedColumn
        && secondColumn instanceof RLEColumn
        && thirdColumn instanceof RLEColumn) {
      doTransformCRR(firstColumn, secondColumn, thirdColumn, builder, positionCount);
      return;
    } else if (firstColumn instanceof RLEColumn
        && secondColumn instanceof RLEColumn
        && thirdColumn instanceof RunLengthEncodedColumn) {
      doTransformRRC(firstColumn, secondColumn, thirdColumn, builder, positionCount);
      return;
    } else if (firstColumn instanceof RLEColumn
        && secondColumn instanceof RLEColumn
        && thirdColumn instanceof RLEColumn) {
      doTransformRRR(firstColumn, secondColumn, thirdColumn, builder, positionCount);
      return;
    }
    doTransformElse(firstColumn, secondColumn, thirdColumn, builder, positionCount);
  }

  private void doTransformRRR(
      Column firstColumn,
      Column secondColumn,
      Column thirdColumn,
      ColumnBuilder builder,
      int positionCount) {
    Pair<Column[], int[]> leftPatterns = ((RLEColumn) firstColumn).getVisibleColumns();
    Pair<Column[], int[]> midPatterns = ((RLEColumn) secondColumn).getVisibleColumns();
    Pair<Column[], int[]> rightPatterns = ((RLEColumn) thirdColumn).getVisibleColumns();
    int leftPatternCount = leftPatterns.getLeft().length;
    int midPatternCount = midPatterns.getLeft().length;
    int rightPatternCount = rightPatterns.getLeft().length;
    int leftIndex = 0, midIndex = 0, rightIndex = 0;
    int curLeft = 0, curMid = 0, curRight = 0;
    int curLeftPositionCount = 0, curMidPositionCount = 0, curRightPositionCount = 0;
    Column leftPatternColumn = leftPatterns.getLeft()[0];
    Column midPatternColumn = midPatterns.getLeft()[0];
    Column rightPatternColumn = rightPatterns.getLeft()[0];
    boolean isRLELeft = true, isRLEMid = true, isRLERight = true;
    int index = 0;
    int length = 0;

    while (index < positionCount) {
      if (curLeft == curLeftPositionCount) {
        if (leftIndex < leftPatternCount) {
          curLeft = 0;
          leftPatternColumn = leftPatterns.getLeft()[leftIndex];
          curLeftPositionCount = leftPatterns.getRight()[leftIndex];
          isRLELeft = leftPatternColumn.getPositionCount() == 1;
          leftIndex++;
        } else {
          if (midIndex < midPatternCount - 1 || rightIndex < rightPatternCount - 1) {
            throw new RuntimeException("3 columns have unequal length");
          } else {
            break;
          }
        }
      }

      if (curMid == curMidPositionCount) {
        if (midIndex < midPatternCount) {
          curMid = 0;
          midPatternColumn = midPatterns.getLeft()[midIndex];
          curMidPositionCount = midPatterns.getRight()[midIndex];
          isRLEMid = midPatternColumn.getPositionCount() == 1;
          midIndex++;
        } else {
          if (leftIndex < leftPatternCount - 1 || rightIndex < rightPatternCount - 1) {
            throw new RuntimeException("3 columns have unequal length");
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
          if (leftIndex < leftPatternCount - 1 || midIndex < midPatternCount - 1) {
            throw new RuntimeException("3 columns have unequal length");
          } else {
            break;
          }
        }
      }

      length =
          Math.min(
              curLeftPositionCount - curLeft,
              Math.min(curMidPositionCount - curMid, curRightPositionCount - curRight));
      length = Math.min(length, positionCount - index);

      if (isRLELeft && isRLEMid && isRLERight) {
        ColumnBuilder columnBuilderTmp = returnType.createColumnBuilder(1);
        boolean flag = false;
        if (!leftPatternColumn.isNull(0)
            && !midPatternColumn.isNull(0)
            && !rightPatternColumn.isNull(0)) {
          if (firstColumnTransformer.getType() instanceof BinaryType) {
            flag =
                flagForBinaryComp(leftPatternColumn, 0, midPatternColumn, 0, rightPatternColumn, 0);
          } else {
            flag =
                flagForDoubleComp(leftPatternColumn, 0, midPatternColumn, 0, rightPatternColumn, 0);
          }
          returnType.writeBoolean(columnBuilderTmp, flag);
        } else {
          columnBuilderTmp.appendNull();
        }
        ((RLEColumnBuilder) builder).writeRLEPattern(columnBuilderTmp.build(), length);
        index += length;
        curLeft += length;
        curMid += length;
        curRight += length;
      } else if (isRLELeft && isRLEMid) {
        ColumnBuilder columnBuilderTmp = returnType.createColumnBuilder(1);
        boolean flag = false;
        if (!leftPatternColumn.isNull(0) && !midPatternColumn.isNull(0)) {
          if (firstColumnTransformer.getType() instanceof BinaryType) {
            for (int i = 0; i < length; i++, curRight++, index++) {
              if (!rightPatternColumn.isNull(curRight)) {
                flag =
                    flagForBinaryComp(
                        leftPatternColumn, 0, midPatternColumn, 0, rightPatternColumn, curRight);
              } else {
                columnBuilderTmp.appendNull();
              }
              returnType.writeBoolean(columnBuilderTmp, flag);
            }
          } else {
            for (int i = 0; i < length; i++, curRight++, index++) {
              if (!rightPatternColumn.isNull(curRight)) {
                flag =
                    flagForDoubleComp(
                        leftPatternColumn, 0, midPatternColumn, 0, rightPatternColumn, curRight);
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
        curMid += length;
      } else if (isRLELeft && isRLERight) {
        ColumnBuilder columnBuilderTmp = returnType.createColumnBuilder(1);
        boolean flag = false;
        if (!leftPatternColumn.isNull(0) && !rightPatternColumn.isNull(0)) {
          if (firstColumnTransformer.getType() instanceof BinaryType) {
            for (int i = 0; i < length; i++, curMid++, index++) {
              if (!midPatternColumn.isNull(curMid)) {
                flag =
                    flagForBinaryComp(
                        leftPatternColumn, 0, midPatternColumn, curMid, rightPatternColumn, 0);
              } else {
                columnBuilderTmp.appendNull();
              }
              returnType.writeBoolean(columnBuilderTmp, flag);
            }
          } else {
            for (int i = 0; i < length; i++, curMid++, index++) {
              if (!midPatternColumn.isNull(curMid)) {
                flag =
                    flagForDoubleComp(
                        leftPatternColumn, 0, midPatternColumn, curMid, rightPatternColumn, 0);
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
        curRight += length;
      } else if (isRLEMid && isRLERight) {
        ColumnBuilder columnBuilderTmp = returnType.createColumnBuilder(1);
        boolean flag = false;
        if (!midPatternColumn.isNull(0) && !rightPatternColumn.isNull(0)) {
          if (firstColumnTransformer.getType() instanceof BinaryType) {
            for (int i = 0; i < length; i++, curLeft++, index++) {
              if (!leftPatternColumn.isNull(curLeft)) {
                flag =
                    flagForBinaryComp(
                        leftPatternColumn, curLeft, midPatternColumn, 0, rightPatternColumn, 0);
              } else {
                columnBuilderTmp.appendNull();
              }
              returnType.writeBoolean(columnBuilderTmp, flag);
            }
          } else {
            for (int i = 0; i < length; i++, curLeft++, index++) {
              if (!leftPatternColumn.isNull(curLeft)) {
                flag =
                    flagForDoubleComp(
                        leftPatternColumn, curLeft, midPatternColumn, 0, rightPatternColumn, 0);
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
        curMid += length;
        curRight += length;
      } else if (isRLELeft) {
        ColumnBuilder columnBuilderTmp = returnType.createColumnBuilder(1);
        boolean flag = false;
        if (!leftPatternColumn.isNull(0)) {
          if (firstColumnTransformer.getType() instanceof BinaryType) {
            for (int i = 0; i < length; i++, curMid++, curRight++, index++) {
              if (!midPatternColumn.isNull(curMid) && !rightPatternColumn.isNull(curRight)) {
                flag =
                    flagForBinaryComp(
                        leftPatternColumn,
                        0,
                        midPatternColumn,
                        curMid,
                        rightPatternColumn,
                        curRight);
              } else {
                columnBuilderTmp.appendNull();
              }
              returnType.writeBoolean(columnBuilderTmp, flag);
            }
          } else {
            for (int i = 0; i < length; i++, curMid++, curRight++, index++) {
              if (!midPatternColumn.isNull(curMid) && !rightPatternColumn.isNull(curRight)) {
                flag =
                    flagForDoubleComp(
                        leftPatternColumn,
                        0,
                        midPatternColumn,
                        curMid,
                        rightPatternColumn,
                        curRight);
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
      } else if (isRLEMid) {
        ColumnBuilder columnBuilderTmp = returnType.createColumnBuilder(1);
        boolean flag = false;
        if (!midPatternColumn.isNull(0)) {
          if (firstColumnTransformer.getType() instanceof BinaryType) {
            for (int i = 0; i < length; i++, curLeft++, curRight++, index++) {
              if (!leftPatternColumn.isNull(curLeft) && !rightPatternColumn.isNull(curRight)) {
                flag =
                    flagForBinaryComp(
                        leftPatternColumn,
                        curLeft,
                        midPatternColumn,
                        0,
                        rightPatternColumn,
                        curRight);
              } else {
                columnBuilderTmp.appendNull();
              }
              returnType.writeBoolean(columnBuilderTmp, flag);
            }
          } else {
            for (int i = 0; i < length; i++, curLeft++, curRight++, index++) {
              if (!leftPatternColumn.isNull(curLeft) && !rightPatternColumn.isNull(curRight)) {
                flag =
                    flagForDoubleComp(
                        leftPatternColumn,
                        curLeft,
                        midPatternColumn,
                        0,
                        rightPatternColumn,
                        curRight);
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
        curMid += length;
      } else if (isRLERight) {
        ColumnBuilder columnBuilderTmp = returnType.createColumnBuilder(1);
        boolean flag = false;
        if (!rightPatternColumn.isNull(0)) {
          if (firstColumnTransformer.getType() instanceof BinaryType) {
            for (int i = 0; i < length; i++, curLeft++, curMid++, index++) {
              if (!leftPatternColumn.isNull(curLeft) && !midPatternColumn.isNull(curMid)) {
                flag =
                    flagForBinaryComp(
                        leftPatternColumn,
                        curLeft,
                        midPatternColumn,
                        curMid,
                        rightPatternColumn,
                        0);
              } else {
                columnBuilderTmp.appendNull();
              }
              returnType.writeBoolean(columnBuilderTmp, flag);
            }
          } else {
            for (int i = 0; i < length; i++, curLeft++, curMid++, index++) {
              if (!leftPatternColumn.isNull(curLeft) && !midPatternColumn.isNull(curMid)) {
                flag =
                    flagForDoubleComp(
                        leftPatternColumn,
                        curLeft,
                        midPatternColumn,
                        curMid,
                        rightPatternColumn,
                        0);
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
        for (int i = 0; i < length; i++, curLeft++, curMid++, curRight++, index++) {
          if (!leftPatternColumn.isNull(curLeft)
              && !midPatternColumn.isNull(curMid)
              && !rightPatternColumn.isNull(curRight)) {
            if (firstColumnTransformer.getType() instanceof BinaryType) {
              flag =
                  flagForBinaryComp(
                      leftPatternColumn,
                      curLeft,
                      midPatternColumn,
                      curMid,
                      rightPatternColumn,
                      curRight);
            } else {
              flag =
                  flagForDoubleComp(
                      leftPatternColumn,
                      curLeft,
                      midPatternColumn,
                      curMid,
                      rightPatternColumn,
                      curRight);
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

  private void doTransformRRC(
      Column firstColumn,
      Column secondColumn,
      Column thirdColumn,
      ColumnBuilder builder,
      int positionCount) {
    Pair<Column[], int[]> leftPatterns = ((RLEColumn) firstColumn).getVisibleColumns();
    Pair<Column[], int[]> midPatterns = ((RLEColumn) secondColumn).getVisibleColumns();
    int leftPatternCount = leftPatterns.getLeft().length;
    int midPatternCount = midPatterns.getLeft().length;
    int leftIndex = 0, midIndex = 0;
    int curLeft = 0, curMid = 0;
    int curLeftPositionCount = 0, curMidPositionCount = 0;
    Column leftPatternColumn = leftPatterns.getLeft()[0];
    Column midPatternColumn = midPatterns.getLeft()[0];
    boolean isRLELeft = true, isRLEMid = true;
    int index = 0;
    int length = 0;

    if (thirdColumn.isNull(0)) {
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
          if (midIndex < midPatternCount - 1) {
            throw new RuntimeException("leftColumn and midColumn have unequal length");
          } else {
            break;
          }
        }
      }

      if (curMid == curMidPositionCount) {
        if (midIndex < midPatternCount) {
          curMid = 0;
          midPatternColumn = midPatterns.getLeft()[midIndex];
          curMidPositionCount = midPatterns.getRight()[midIndex];
          isRLEMid = midPatternColumn.getPositionCount() == 1;
          midIndex++;
        } else {
          if (leftIndex < leftPatternCount - 1) {
            throw new RuntimeException("leftColumn and midColumn have unequal length");
          } else {
            break;
          }
        }
      }

      length =
          curLeftPositionCount - curLeft > curMidPositionCount - curMid
              ? curMidPositionCount - curMid
              : curLeftPositionCount - curLeft;
      length = length > positionCount - index ? positionCount - index : length;

      if (isRLELeft && isRLEMid) {
        ColumnBuilder columnBuilderTmp = returnType.createColumnBuilder(1);
        boolean flag = false;
        if (!leftPatternColumn.isNull(0) && !midPatternColumn.isNull(0)) {
          if (firstColumnTransformer.getType() instanceof BinaryType) {
            flag = flagForBinaryComp(leftPatternColumn, 0, midPatternColumn, 0, thirdColumn, 0);
          } else {
            flag = flagForDoubleComp(leftPatternColumn, 0, midPatternColumn, 0, thirdColumn, 0);
          }
          returnType.writeBoolean(columnBuilderTmp, flag);
        } else {
          columnBuilderTmp.appendNull();
        }
        ((RLEColumnBuilder) builder).writeRLEPattern(columnBuilderTmp.build(), length);
        index += length;
        curLeft += length;
        curMid += length;
      } else if (isRLELeft) {
        ColumnBuilder columnBuilderTmp = returnType.createColumnBuilder(1);
        boolean flag = false;
        if (!leftPatternColumn.isNull(0)) {
          if (firstColumnTransformer.getType() instanceof BinaryType) {
            for (int i = 0; i < length; i++, curMid++, index++) {
              if (!midPatternColumn.isNull(curMid)) {
                flag =
                    flagForBinaryComp(
                        leftPatternColumn, 0, midPatternColumn, curMid, thirdColumn, 0);
              } else {
                columnBuilderTmp.appendNull();
              }
              returnType.writeBoolean(columnBuilderTmp, flag);
            }
          } else {
            for (int i = 0; i < length; i++, curMid++, index++) {
              if (!midPatternColumn.isNull(curMid)) {
                flag =
                    flagForDoubleComp(
                        leftPatternColumn, 0, midPatternColumn, curMid, thirdColumn, 0);
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
      } else if (isRLEMid) {
        ColumnBuilder columnBuilderTmp = returnType.createColumnBuilder(1);
        boolean flag = false;
        if (!midPatternColumn.isNull(0)) {
          if (firstColumnTransformer.getType() instanceof BinaryType) {
            for (int i = 0; i < length; i++, curLeft++, index++) {
              if (!leftPatternColumn.isNull(curLeft)) {
                flag =
                    flagForBinaryComp(
                        leftPatternColumn, curLeft, midPatternColumn, 0, thirdColumn, 0);
              } else {
                columnBuilderTmp.appendNull();
              }
              returnType.writeBoolean(columnBuilderTmp, flag);
            }
          } else {
            for (int i = 0; i < length; i++, curLeft++, index++) {
              if (!leftPatternColumn.isNull(curLeft)) {
                flag =
                    flagForDoubleComp(
                        leftPatternColumn, curLeft, midPatternColumn, 0, thirdColumn, 0);
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
        curMid += length;
      } else {
        ColumnBuilder columnBuilderTmp = returnType.createColumnBuilder(1);
        boolean flag = false;
        for (int i = 0; i < length; i++, curLeft++, curMid++, index++) {
          if (!leftPatternColumn.isNull(curLeft) && !midPatternColumn.isNull(curMid)) {
            if (firstColumnTransformer.getType() instanceof BinaryType) {
              flag =
                  flagForBinaryComp(
                      leftPatternColumn, curLeft, midPatternColumn, curMid, thirdColumn, 0);
            } else {
              flag =
                  flagForDoubleComp(
                      leftPatternColumn, curLeft, midPatternColumn, curMid, thirdColumn, 0);
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

  private void doTransformCRR(
      Column firstColumn,
      Column secondColumn,
      Column thirdColumn,
      ColumnBuilder builder,
      int positionCount) {
    Pair<Column[], int[]> midPatterns = ((RLEColumn) secondColumn).getVisibleColumns();
    Pair<Column[], int[]> rightPatterns = ((RLEColumn) thirdColumn).getVisibleColumns();
    int midPatternCount = midPatterns.getLeft().length;
    int rightPatternCount = rightPatterns.getLeft().length;
    int midIndex = 0, rightIndex = 0;
    int curMid = 0, curRight = 0;
    int curMidPositionCount = 0, curRightPositionCount = 0;
    Column midPatternColumn = midPatterns.getLeft()[0];
    Column rightPatternColumn = rightPatterns.getLeft()[0];
    boolean isRLEMid = true, isRLERight = true;
    int index = 0;
    int length = 0;

    if (firstColumn.isNull(0)) {
      ColumnBuilder columnBuilderTmp = returnType.createColumnBuilder(1);
      columnBuilderTmp.appendNull();
      ((RLEColumnBuilder) builder).writeRLEPattern(columnBuilderTmp.build(), positionCount);
      return;
    }

    while (index < positionCount) {
      if (curMid == curMidPositionCount) {
        if (midIndex < midPatternCount) {
          curMid = 0;
          midPatternColumn = midPatterns.getLeft()[midIndex];
          curMidPositionCount = midPatterns.getRight()[midIndex];
          isRLEMid = midPatternColumn.getPositionCount() == 1;
          midIndex++;
        } else {
          if (rightIndex < rightPatternCount - 1) {
            throw new RuntimeException("midColumn and rightColumn have unequal length");
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
          if (midIndex < midPatternCount - 1) {
            throw new RuntimeException("midColumn and rightColumn have unequal length");
          } else {
            break;
          }
        }
      }
      length =
          curMidPositionCount - curMid > curRightPositionCount - curRight
              ? curRightPositionCount - curRight
              : curMidPositionCount - curMid;
      length = length > positionCount - index ? positionCount - index : length;

      if (isRLEMid && isRLERight) {
        ColumnBuilder columnBuilderTmp = returnType.createColumnBuilder(1);
        boolean flag = false;
        if (!midPatternColumn.isNull(0) && !rightPatternColumn.isNull(0)) {
          if (firstColumnTransformer.getType() instanceof BinaryType) {
            flag = flagForBinaryComp(firstColumn, 0, midPatternColumn, 0, rightPatternColumn, 0);
          } else {
            flag = flagForDoubleComp(firstColumn, 0, midPatternColumn, 0, rightPatternColumn, 0);
          }
          returnType.writeBoolean(columnBuilderTmp, flag);
        } else {
          columnBuilderTmp.appendNull();
        }
        ((RLEColumnBuilder) builder).writeRLEPattern(columnBuilderTmp.build(), length);
        index += length;
        curRight += length;
        curMid += length;
      } else if (isRLEMid) {
        ColumnBuilder columnBuilderTmp = returnType.createColumnBuilder(1);
        boolean flag = false;
        if (!midPatternColumn.isNull(0)) {
          if (firstColumnTransformer.getType() instanceof BinaryType) {
            for (int i = 0; i < length; i++, curRight++, index++) {
              if (!rightPatternColumn.isNull(curRight)) {
                flag =
                    flagForBinaryComp(
                        firstColumn, 0, midPatternColumn, 0, rightPatternColumn, curRight);
              } else {
                columnBuilderTmp.appendNull();
              }
              returnType.writeBoolean(columnBuilderTmp, flag);
            }
          } else {
            for (int i = 0; i < length; i++, curRight++, index++) {
              if (!rightPatternColumn.isNull(curRight)) {
                flag =
                    flagForDoubleComp(
                        firstColumn, 0, midPatternColumn, 0, rightPatternColumn, curRight);
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
        curMid += length;
      } else if (isRLERight) {
        ColumnBuilder columnBuilderTmp = returnType.createColumnBuilder(1);
        boolean flag = false;
        if (!rightPatternColumn.isNull(0)) {
          if (firstColumnTransformer.getType() instanceof BinaryType) {
            for (int i = 0; i < length; i++, curMid++, index++) {
              if (!midPatternColumn.isNull(curMid)) {
                flag =
                    flagForBinaryComp(
                        firstColumn, 0, midPatternColumn, curMid, rightPatternColumn, 0);
              } else {
                columnBuilderTmp.appendNull();
              }
              returnType.writeBoolean(columnBuilderTmp, flag);
            }
          } else {
            for (int i = 0; i < length; i++, curMid++, index++) {
              if (!midPatternColumn.isNull(curMid)) {
                flag =
                    flagForDoubleComp(
                        firstColumn, 0, midPatternColumn, curMid, rightPatternColumn, 0);
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
        for (int i = 0; i < length; i++, curMid++, curRight++, index++) {
          if (!midPatternColumn.isNull(curMid) && !rightPatternColumn.isNull(curRight)) {
            if (firstColumnTransformer.getType() instanceof BinaryType) {
              flag =
                  flagForBinaryComp(
                      firstColumn, 0, midPatternColumn, curMid, rightPatternColumn, curRight);
            } else {
              flag =
                  flagForDoubleComp(
                      firstColumn, 0, midPatternColumn, curMid, rightPatternColumn, curRight);
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
            flag = flagForBinaryComp(leftPatternColumn, 0, secondColumn, 0, rightPatternColumn, 0);
          } else {
            flag = flagForDoubleComp(leftPatternColumn, 0, secondColumn, 0, rightPatternColumn, 0);
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
                    flagForBinaryComp(
                        leftPatternColumn, 0, secondColumn, 0, rightPatternColumn, curRight);
              } else {
                columnBuilderTmp.appendNull();
              }
              returnType.writeBoolean(columnBuilderTmp, flag);
            }
          } else {
            for (int i = 0; i < length; i++, curRight++, index++) {
              if (!rightPatternColumn.isNull(curRight)) {
                flag =
                    flagForDoubleComp(
                        leftPatternColumn, 0, secondColumn, 0, rightPatternColumn, curRight);
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
                    flagForBinaryComp(
                        leftPatternColumn, curLeft, secondColumn, 0, rightPatternColumn, 0);
              } else {
                columnBuilderTmp.appendNull();
              }
              returnType.writeBoolean(columnBuilderTmp, flag);
            }
          } else {
            for (int i = 0; i < length; i++, curLeft++, index++) {
              if (!leftPatternColumn.isNull(curLeft)) {
                flag =
                    flagForDoubleComp(
                        leftPatternColumn, curLeft, secondColumn, 0, rightPatternColumn, 0);
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
                  flagForBinaryComp(
                      leftPatternColumn, curLeft, secondColumn, 0, rightPatternColumn, curRight);
            } else {
              flag =
                  flagForDoubleComp(
                      leftPatternColumn, curLeft, secondColumn, 0, rightPatternColumn, curRight);
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
            flag = flagForBinaryComp(firstColumn, 0, secondColumn, 0, rightPatternColumn, 0);
          } else {
            flag = flagForDoubleComp(firstColumn, 0, secondColumn, 0, rightPatternColumn, 0);
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
                  flagForBinaryComp(firstColumn, 0, secondColumn, 0, rightPatternColumn, curRight);
            } else {
              columnBuilderTmp.appendNull();
            }
            returnType.writeBoolean(columnBuilderTmp, flag);
          }
        } else {
          for (int i = 0; i < length; i++, curRight++, index++) {
            if (!rightPatternColumn.isNull(curRight)) {
              flag =
                  flagForDoubleComp(firstColumn, 0, secondColumn, 0, rightPatternColumn, curRight);
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
              "The positionCount of midColumn is less than the requested positionCount");
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
            flag = flagForBinaryComp(firstColumn, 0, midPatternColumn, 0, thirdColumn, 0);
          } else {
            flag = flagForDoubleComp(firstColumn, 0, midPatternColumn, 0, thirdColumn, 0);
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
              flag = flagForBinaryComp(firstColumn, 0, midPatternColumn, curMid, thirdColumn, 0);
            } else {
              columnBuilderTmp.appendNull();
            }
            returnType.writeBoolean(columnBuilderTmp, flag);
          }
        } else {
          for (int i = 0; i < length; i++, curMid++, index++) {
            if (!midPatternColumn.isNull(curMid)) {
              flag = flagForDoubleComp(firstColumn, 0, midPatternColumn, curMid, thirdColumn, 0);
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
              "The positionCount of leftColumn is less than the requested positionCount");
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
            flag = flagForBinaryComp(leftPatternColumn, 0, secondColumn, 0, thirdColumn, 0);
          } else {
            flag = flagForDoubleComp(leftPatternColumn, 0, secondColumn, 0, thirdColumn, 0);
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
              flag = flagForBinaryComp(leftPatternColumn, curLeft, secondColumn, 0, thirdColumn, 0);
            } else {
              columnBuilderTmp.appendNull();
            }
            returnType.writeBoolean(columnBuilderTmp, flag);
          }
        } else {
          for (int i = 0; i < length; i++, curLeft++, index++) {
            if (!leftPatternColumn.isNull(curLeft)) {
              flag = flagForDoubleComp(leftPatternColumn, curLeft, secondColumn, 0, thirdColumn, 0);
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
          flag = flagForBinaryComp(firstColumn, i, secondColumn, i, thirdColumn, i);
        } else {
          flag = flagForDoubleComp(firstColumn, i, secondColumn, i, thirdColumn, i);
        }
        returnType.writeBoolean(builder, flag);
      } else {
        builder.appendNull();
      }
    }
  }

  private boolean flagForBinaryComp(
      Column firstColumn,
      int firstPos,
      Column secondColumn,
      int secondPos,
      Column thirdColumn,
      int thirdPos) {
    return ((TransformUtils.compare(
                    firstColumnTransformer.getType().getBinary(firstColumn, firstPos),
                    secondColumnTransformer.getType().getBinary(secondColumn, secondPos))
                >= 0)
            && (TransformUtils.compare(
                    firstColumnTransformer.getType().getBinary(firstColumn, firstPos),
                    thirdColumnTransformer.getType().getBinary(thirdColumn, thirdPos))
                <= 0))
        ^ isNotBetween;
  }

  private boolean flagForDoubleComp(
      Column firstColumn,
      int firstPos,
      Column secondColumn,
      int secondPos,
      Column thirdColumn,
      int thirdPos) {
    return ((Double.compare(
                    firstColumnTransformer.getType().getDouble(firstColumn, firstPos),
                    secondColumnTransformer.getType().getDouble(secondColumn, secondPos))
                >= 0)
            && (Double.compare(
                    firstColumnTransformer.getType().getDouble(firstColumn, firstPos),
                    thirdColumnTransformer.getType().getDouble(thirdColumn, thirdPos))
                <= 0))
        ^ isNotBetween;
  }
}
