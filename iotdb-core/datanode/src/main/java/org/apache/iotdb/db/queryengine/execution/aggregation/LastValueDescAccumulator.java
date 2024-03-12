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
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.RLEColumn;
import org.apache.iotdb.tsfile.read.common.block.column.RLEPatternColumn;
import org.apache.iotdb.tsfile.utils.BitMap;

public class LastValueDescAccumulator extends LastValueAccumulator {

  public LastValueDescAccumulator(TSDataType seriesDataType) {
    super(seriesDataType);
  }

  @Override
  public boolean hasFinalResult() {
    return initResult;
  }

  @Override
  public void reset() {
    super.reset();
  }

  @Override
  protected void addIntInput(Column[] column, BitMap needSkip, int lastIndex) {
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
          if (curPattern.isRLEMode()) {
            for (int j = 0; j < curPatternCount; j++, curIndex++) {
              if (needSkip != null && !needSkip.isMarked(j)) {
                continue;
              }
              updateIntLastValue(curPattern.getInt(0), column[0].getLong(curIndex));
              return;
            }
          } else {
            for (int j = 0; j < curPatternCount; j++, curIndex++) {
              if (needSkip != null && !needSkip.isMarked(j)) {
                continue;
              }
              if (!curPattern.isNull(j)) {
                updateIntLastValue(curPattern.getInt(j), column[0].getLong(curIndex));
                return;
              }
            }
          }
        }
      }
      return;
    }
    for (int i = 0; i <= lastIndex; i++) {
      // skip null value in control column
      if (needSkip != null && needSkip.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        updateIntLastValue(column[1].getInt(i), column[0].getLong(i));
        return;
      }
    }
  }

  @Override
  protected void addLongInput(Column[] column, BitMap needSkip, int lastIndex) {
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
          if (curPattern.isRLEMode()) {
            for (int j = 0; j < curPatternCount; j++, curIndex++) {
              if (needSkip != null && !needSkip.isMarked(j)) {
                continue;
              }
              updateLongLastValue(curPattern.getLong(0), column[0].getLong(curIndex));
              return;
            }
          } else {
            for (int j = 0; j < curPatternCount; j++, curIndex++) {
              if (needSkip != null && !needSkip.isMarked(j)) {
                continue;
              }
              if (!curPattern.isNull(j)) {
                updateLongLastValue(curPattern.getLong(j), column[0].getLong(curIndex));
                return;
              }
            }
          }
        }
      }
      return;
    }
    for (int i = 0; i <= lastIndex; i++) {
      // skip null value in control column
      if (needSkip != null && needSkip.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        updateLongLastValue(column[1].getLong(i), column[0].getLong(i));
        return;
      }
    }
  }

  @Override
  protected void addFloatInput(Column[] column, BitMap needSkip, int lastIndex) {
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
          if (curPattern.isRLEMode()) {
            for (int j = 0; j < curPatternCount; j++, curIndex++) {
              if (needSkip != null && !needSkip.isMarked(j)) {
                continue;
              }
              updateFloatLastValue(curPattern.getFloat(0), column[0].getLong(curIndex));
              return;
            }
          } else {
            for (int j = 0; j < curPatternCount; j++, curIndex++) {
              if (needSkip != null && !needSkip.isMarked(j)) {
                continue;
              }
              if (!curPattern.isNull(j)) {
                updateFloatLastValue(curPattern.getFloat(j), column[0].getLong(curIndex));
                return;
              }
            }
          }
        }
      }
      return;
    }
    for (int i = 0; i <= lastIndex; i++) {
      // skip null value in control column
      if (needSkip != null && needSkip.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        updateFloatLastValue(column[1].getFloat(i), column[0].getLong(i));
        return;
      }
    }
  }

  @Override
  protected void addDoubleInput(Column[] column, BitMap needSkip, int lastIndex) {
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
          if (curPattern.isRLEMode()) {
            for (int j = 0; j < curPatternCount; j++, curIndex++) {
              if (needSkip != null && !needSkip.isMarked(j)) {
                continue;
              }
              updateDoubleLastValue(curPattern.getDouble(0), column[0].getLong(curIndex));
              return;
            }
          } else {
            for (int j = 0; j < curPatternCount; j++, curIndex++) {
              if (needSkip != null && !needSkip.isMarked(j)) {
                continue;
              }
              if (!curPattern.isNull(j)) {
                updateDoubleLastValue(curPattern.getDouble(j), column[0].getLong(curIndex));
                return;
              }
            }
          }
        }
      }
      return;
    }
    for (int i = 0; i <= lastIndex; i++) {
      // skip null value in control column
      if (needSkip != null && needSkip.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        updateDoubleLastValue(column[1].getDouble(i), column[0].getLong(i));
        return;
      }
    }
  }

  @Override
  protected void addBooleanInput(Column[] column, BitMap needSkip, int lastIndex) {
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
          if (curPattern.isRLEMode()) {
            for (int j = 0; j < curPatternCount; j++, curIndex++) {
              if (needSkip != null && !needSkip.isMarked(j)) {
                continue;
              }
              updateBooleanLastValue(curPattern.getBoolean(0), column[0].getLong(curIndex));
              return;
            }
          } else {
            for (int j = 0; j < curPatternCount; j++, curIndex++) {
              if (needSkip != null && !needSkip.isMarked(j)) {
                continue;
              }
              if (!curPattern.isNull(j)) {
                updateBooleanLastValue(curPattern.getBoolean(j), column[0].getLong(curIndex));
                return;
              }
            }
          }
        }
      }
      return;
    }
    for (int i = 0; i <= lastIndex; i++) {
      // skip null value in control column
      if (needSkip != null && needSkip.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        updateBooleanLastValue(column[1].getBoolean(i), column[0].getLong(i));
        return;
      }
    }
  }

  @Override
  protected void addBinaryInput(Column[] column, BitMap needSkip, int lastIndex) {
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
          if (curPattern.isRLEMode()) {
            for (int j = 0; j < curPatternCount; j++, curIndex++) {
              if (needSkip != null && !needSkip.isMarked(j)) {
                continue;
              }
              updateBinaryLastValue(curPattern.getBinary(0), column[0].getLong(curIndex));
              return;
            }
          } else {
            for (int j = 0; j < curPatternCount; j++, curIndex++) {
              if (needSkip != null && !needSkip.isMarked(j)) {
                continue;
              }
              if (!curPattern.isNull(j)) {
                updateBinaryLastValue(curPattern.getBinary(j), column[0].getLong(curIndex));
                return;
              }
            }
          }
        }
      }
      return;
    }
    for (int i = 0; i <= lastIndex; i++) {
      // skip null value in control column
      if (needSkip != null && needSkip.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        updateBinaryLastValue(column[1].getBinary(i), column[0].getLong(i));
        return;
      }
    }
  }
}
