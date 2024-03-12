/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.aggregation;

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.IntegerStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.RLEColumn;
import org.apache.iotdb.tsfile.read.common.block.column.RLEPatternColumn;
import org.apache.iotdb.tsfile.utils.BitMap;

import static com.google.common.base.Preconditions.checkArgument;

public class SumAccumulator implements Accumulator {

  private final TSDataType seriesDataType;
  private double sumValue = 0;
  private boolean initResult = false;

  public SumAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
  }

  // Column should be like: | Time | Value |
  @Override
  public void addInput(Column[] column, BitMap bitMap, int lastIndex) {
    switch (seriesDataType) {
      case INT32:
        addIntInput(column, bitMap, lastIndex);
        return;
      case INT64:
        addLongInput(column, bitMap, lastIndex);
        return;
      case FLOAT:
        addFloatInput(column, bitMap, lastIndex);
        return;
      case DOUBLE:
        addDoubleInput(column, bitMap, lastIndex);
        return;
      case TEXT:
      case BOOLEAN:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in aggregation AVG : %s", seriesDataType));
    }
  }

  // partialResult should be like: | partialSumValue1 |
  @Override
  public void addIntermediate(Column[] partialResult) {
    checkArgument(partialResult.length == 1, "partialResult of Sum should be 1");
    if (partialResult[0].isNull(0)) {
      return;
    }
    initResult = true;
    sumValue += partialResult[0].getDouble(0);
  }

  @Override
  public void removeIntermediate(Column[] input) {
    checkArgument(input.length == 1, "input of Sum should be 1");
    if (input[0].isNull(0)) {
      return;
    }
    sumValue -= input[0].getDouble(0);
  }

  @Override
  public void addStatistics(Statistics statistics) {
    if (statistics == null) {
      return;
    }
    initResult = true;
    if (statistics instanceof IntegerStatistics) {
      sumValue += statistics.getSumLongValue();
    } else {
      sumValue += statistics.getSumDoubleValue();
    }
  }

  // finalResult should be single column, like: | finalSumValue |
  @Override
  public void setFinal(Column finalResult) {
    reset();
    if (finalResult.isNull(0)) {
      return;
    }
    initResult = true;
    sumValue = finalResult.getDouble(0);
  }

  // columnBuilder should be single in countAccumulator
  @Override
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    checkArgument(columnBuilders.length == 1, "partialResult of Sum should be 1");
    if (!initResult) {
      columnBuilders[0].appendNull();
    } else {
      columnBuilders[0].writeDouble(sumValue);
    }
  }

  @Override
  public void outputFinal(ColumnBuilder columnBuilder) {
    if (!initResult) {
      columnBuilder.appendNull();
    } else {
      columnBuilder.writeDouble(sumValue);
    }
  }

  @Override
  public void reset() {
    initResult = false;
    this.sumValue = 0;
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public TSDataType[] getIntermediateType() {
    return new TSDataType[] {TSDataType.DOUBLE};
  }

  @Override
  public TSDataType getFinalType() {
    return TSDataType.DOUBLE;
  }

  private void addIntInput(Column[] column, BitMap bitMap, int lastIndex) {
    if (column[1] instanceof RLEColumn) {
      int curIndex = 0;
      int positionCount = column[1].getPositionCount();
      int curPatternCount = 0;
      for (int i = 0; i < positionCount; i++) {
        if (!((RLEColumn) column[1]).isNullRLE(i)) {
          RLEPatternColumn curPattern = ((RLEColumn) column[1]).getRLEPattern(i);
          curPatternCount = curPattern.getPositionCount();
          curPatternCount =
              curIndex + curPatternCount - 1 <= lastIndex
                  ? curPatternCount
                  : lastIndex - curIndex + 1;
          if (curPattern.isRLEMode()
              && (bitMap == null
                  || bitMap.isAllMarked()
                  || bitMap.getRegion(curIndex, curPatternCount).isAllMarked())) {
            initResult = true;
            sumValue += curPatternCount * curPattern.getInt(0);
            curIndex += curPatternCount;
          } else {
            for (int j = 0; j < curPatternCount; j++, curIndex++) {
              if (bitMap != null && !bitMap.isMarked(curIndex)) {
                continue;
              }
              if (!curPattern.isNull(j)) {
                initResult = true;
                sumValue += curPattern.getInt(j);
              }
            }
          }
        }
      }
      return;
    }
    for (int i = 0; i <= lastIndex; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        initResult = true;
        sumValue += column[1].getInt(i);
      }
    }
  }

  private void addLongInput(Column[] column, BitMap bitMap, int lastIndex) {
    if (column[1] instanceof RLEColumn) {
      int curIndex = 0;
      int positionCount = column[1].getPositionCount();
      int curPatternCount = 0;
      for (int i = 0; i < positionCount; i++) {
        if (!((RLEColumn) column[1]).isNullRLE(i)) {
          RLEPatternColumn curPattern = ((RLEColumn) column[1]).getRLEPattern(i);
          curPatternCount = curPattern.getPositionCount();
          curPatternCount =
              curIndex + curPatternCount - 1 <= lastIndex
                  ? curPatternCount
                  : lastIndex - curIndex + 1;
          if (curPattern.isRLEMode()
              && (bitMap == null
                  || bitMap.isAllMarked()
                  || bitMap.getRegion(curIndex, curPatternCount).isAllMarked())) {
            initResult = true;
            sumValue += curPatternCount * curPattern.getLong(0);
            curIndex += curPatternCount;
          } else {
            for (int j = 0; j < curPatternCount; j++, curIndex++) {
              if (bitMap != null && !bitMap.isMarked(curIndex)) {
                continue;
              }
              if (!curPattern.isNull(j)) {
                initResult = true;
                sumValue += curPattern.getLong(j);
              }
            }
          }
        }
      }
      return;
    }
    for (int i = 0; i <= lastIndex; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        initResult = true;
        sumValue += column[1].getLong(i);
      }
    }
  }

  private void addFloatInput(Column[] column, BitMap bitMap, int lastIndex) {
    if (column[1] instanceof RLEColumn) {
      int curIndex = 0;
      int positionCount = column[1].getPositionCount();
      int curPatternCount = 0;
      for (int i = 0; i < positionCount; i++) {
        if (!((RLEColumn) column[1]).isNullRLE(i)) {
          RLEPatternColumn curPattern = ((RLEColumn) column[1]).getRLEPattern(i);
          curPatternCount = curPattern.getPositionCount();
          curPatternCount =
              curIndex + curPatternCount - 1 <= lastIndex
                  ? curPatternCount
                  : lastIndex - curIndex + 1;
          if (curPattern.isRLEMode()
              && (bitMap == null
                  || bitMap.isAllMarked()
                  || bitMap.getRegion(curIndex, curPatternCount).isAllMarked())) {
            initResult = true;
            sumValue += curPatternCount * curPattern.getFloat(0);
            curIndex += curPatternCount;
          } else {
            for (int j = 0; j < curPatternCount; j++, curIndex++) {
              if (bitMap != null && !bitMap.isMarked(curIndex)) {
                continue;
              }
              if (!curPattern.isNull(j)) {
                initResult = true;
                sumValue += curPattern.getFloat(j);
              }
            }
          }
        }
      }
      return;
    }
    for (int i = 0; i <= lastIndex; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        initResult = true;
        sumValue += column[1].getFloat(i);
      }
    }
  }

  private void addDoubleInput(Column[] column, BitMap bitMap, int lastIndex) {
    if (column[1] instanceof RLEColumn) {
      int curIndex = 0;
      int positionCount = column[1].getPositionCount();
      int curPatternCount = 0;
      for (int i = 0; i < positionCount; i++) {
        if (!((RLEColumn) column[1]).isNullRLE(i)) {
          RLEPatternColumn curPattern = ((RLEColumn) column[1]).getRLEPattern(i);
          curPatternCount = curPattern.getPositionCount();
          curPatternCount =
              curIndex + curPatternCount - 1 <= lastIndex
                  ? curPatternCount
                  : lastIndex - curIndex + 1;
          if (curPattern.isRLEMode()
              && (bitMap == null
                  || bitMap.isAllMarked()
                  || bitMap.getRegion(curIndex, curPatternCount).isAllMarked())) {
            initResult = true;
            sumValue += curPatternCount * curPattern.getDouble(0);
            curIndex += curPatternCount;
          } else {
            for (int j = 0; j < curPatternCount; j++, curIndex++) {
              if (bitMap != null && !bitMap.isMarked(curIndex)) {
                continue;
              }
              if (!curPattern.isNull(j)) {
                initResult = true;
                sumValue += curPattern.getDouble(j);
              }
            }
          }
        }
      }
      return;
    }
    for (int i = 0; i <= lastIndex; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        initResult = true;
        sumValue += column[1].getDouble(i);
      }
    }
  }
}
