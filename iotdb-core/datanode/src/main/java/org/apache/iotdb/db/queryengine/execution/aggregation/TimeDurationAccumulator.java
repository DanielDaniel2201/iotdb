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

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.RLEColumn;
import org.apache.iotdb.tsfile.read.common.block.column.RLEPatternColumn;
import org.apache.iotdb.tsfile.utils.BitMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeDurationAccumulator implements Accumulator {
  private static final Logger LOGGER = LoggerFactory.getLogger(CountAccumulator.class);

  protected long minTime = Long.MAX_VALUE;
  protected long maxTime = Long.MIN_VALUE;
  protected boolean initResult = false;

  @Override
  public void addInput(Column[] column, BitMap bitMap, int lastIndex) {
    if (column[1] instanceof RLEColumn) {
      // **********************************************
      LOGGER.info("RLE-addInput branch is chosen");
      // **********************************************
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
          long curValue = -1;
          if (curPattern.isRLEMode()) {
            for (int j = 0; j < curPatternCount; j++, curIndex++) {
              if (bitMap != null && !bitMap.isMarked(curIndex)) {
                continue;
              }
              curValue = curPattern.getLong(0);
              initResult = true;
              updateMaxTime(curValue);
              updateMinTime(curValue);
              curIndex = curIndex - j + curPatternCount;
              break;
            }
          } else {
            for (int j = 0; j < curPatternCount; j++, curIndex++) {
              if (bitMap != null && !bitMap.isMarked(curIndex)) {
                continue;
              }
              if (!curPattern.isNull(j)) {
                curValue = curPattern.getLong(j);
                initResult = true;
                updateMaxTime(curValue);
                updateMinTime(curValue);
              }
            }
          }
        }
      }
      return;
    }
    // **************************************************
    LOGGER.info("non-RLE-addInput branch is chosen");
    // **************************************************
    for (int i = 0; i <= lastIndex; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        initResult = true;
        updateMaxTime(column[0].getLong(i));
        updateMinTime(column[0].getLong(i));
      }
    }
  }

  @Override
  public void addIntermediate(Column[] partialResult) {
    if (partialResult[0].isNull(0)) {
      return;
    }
    initResult = true;
    updateMaxTime(partialResult[0].getLong(0));
    updateMinTime(partialResult[1].getLong(0));
  }

  @Override
  public void addStatistics(Statistics statistics) {
    updateMaxTime(statistics.getEndTime());
    updateMinTime(statistics.getStartTime());
  }

  @Override
  public void setFinal(Column finalResult) {
    if (finalResult.isNull(0)) {
      return;
    }
    initResult = true;
    maxTime = finalResult.getLong(0);
    minTime = 0L;
  }

  @Override
  public void outputIntermediate(ColumnBuilder[] tsBlockBuilder) {
    if (!initResult) {
      tsBlockBuilder[0].appendNull();
      tsBlockBuilder[1].appendNull();
    } else {
      tsBlockBuilder[0].writeLong(maxTime);
      tsBlockBuilder[1].writeLong(minTime);
    }
  }

  @Override
  public void outputFinal(ColumnBuilder tsBlockBuilder) {
    if (!initResult) {
      tsBlockBuilder.appendNull();
    } else {
      tsBlockBuilder.writeLong(maxTime - minTime);
    }
  }

  @Override
  public void reset() {
    initResult = false;
    this.maxTime = 0L;
    this.minTime = Long.MAX_VALUE;
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public TSDataType[] getIntermediateType() {
    return new TSDataType[] {TSDataType.INT64, TSDataType.INT64};
  }

  @Override
  public TSDataType getFinalType() {
    return TSDataType.INT64;
  }

  protected void updateMaxTime(long curTime) {
    initResult = true;
    maxTime = Math.max(maxTime, curTime);
  }

  protected void updateMinTime(long curTime) {
    initResult = true;
    minTime = Math.min(minTime, curTime);
  }
}
