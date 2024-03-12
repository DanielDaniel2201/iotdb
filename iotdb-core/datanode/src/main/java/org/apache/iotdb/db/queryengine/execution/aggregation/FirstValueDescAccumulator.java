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

public class FirstValueDescAccumulator extends FirstValueAccumulator {

  public FirstValueDescAccumulator(TSDataType seriesDataType) {
    super(seriesDataType);
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  // Don't break in advance
  @Override
  protected void addIntInput(Column[] column, BitMap bitMap, int lastIndex) {
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
              if (bitMap != null && !bitMap.isMarked(curIndex)) {
                continue;
              }
              updateIntFirstValue(curPattern.getInt(0), column[0].getLong(curIndex));
            }
          } else {
            for (int j = 0; j < curPatternCount; j++, curIndex++) {
              if (bitMap != null && !bitMap.isMarked(curIndex)) {
                continue;
              }
              if (!curPattern.isNull(j)) {
                updateIntFirstValue(curPattern.getInt(j), column[0].getLong(curIndex));
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
        updateIntFirstValue(column[1].getInt(i), column[0].getLong(i));
      }
    }
  }

  @Override
  protected void addLongInput(Column[] column, BitMap bitMap, int lastIndex) {
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
              if (bitMap != null && !bitMap.isMarked(curIndex)) {
                continue;
              }
              updateLongFirstValue(curPattern.getLong(0), column[0].getLong(curIndex));
            }
          } else {
            for (int j = 0; j < curPatternCount; j++, curIndex++) {
              if (bitMap != null && !bitMap.isMarked(curIndex)) {
                continue;
              }
              if (!curPattern.isNull(j)) {
                updateLongFirstValue(curPattern.getLong(j), column[0].getLong(curIndex));
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
        updateLongFirstValue(column[1].getLong(i), column[0].getLong(i));
      }
    }
  }

  @Override
  protected void addFloatInput(Column[] column, BitMap bitMap, int lastIndex) {
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
              if (bitMap != null && !bitMap.isMarked(curIndex)) {
                continue;
              }
              updateFloatFirstValue(curPattern.getFloat(0), column[0].getLong(curIndex));
            }
          } else {
            for (int j = 0; j < curPatternCount; j++, curIndex++) {
              if (bitMap != null && !bitMap.isMarked(curIndex)) {
                continue;
              }
              if (!curPattern.isNull(j)) {
                updateFloatFirstValue(curPattern.getFloat(j), column[0].getLong(curIndex));
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
        updateFloatFirstValue(column[1].getFloat(i), column[0].getLong(i));
      }
    }
  }

  @Override
  protected void addDoubleInput(Column[] column, BitMap bitMap, int lastIndex) {
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
              if (bitMap != null && !bitMap.isMarked(curIndex)) {
                continue;
              }
              updateDoubleFirstValue(curPattern.getDouble(0), column[0].getLong(curIndex));
            }
          } else {
            for (int j = 0; j < curPatternCount; j++, curIndex++) {
              if (bitMap != null && !bitMap.isMarked(curIndex)) {
                continue;
              }
              if (!curPattern.isNull(j)) {
                updateDoubleFirstValue(curPattern.getDouble(j), column[0].getLong(curIndex));
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
        updateDoubleFirstValue(column[1].getDouble(i), column[0].getLong(i));
      }
    }
  }

  @Override
  protected void addBooleanInput(Column[] column, BitMap bitMap, int lastIndex) {
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
              if (bitMap != null && !bitMap.isMarked(curIndex)) {
                continue;
              }
              updateBooleanFirstValue(curPattern.getBoolean(0), column[0].getLong(curIndex));
            }
          } else {
            for (int j = 0; j < curPatternCount; j++, curIndex++) {
              if (bitMap != null && !bitMap.isMarked(curIndex)) {
                continue;
              }
              if (!curPattern.isNull(j)) {
                updateBooleanFirstValue(curPattern.getBoolean(j), column[0].getLong(curIndex));
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
        updateBooleanFirstValue(column[1].getBoolean(i), column[0].getLong(i));
      }
    }
  }

  @Override
  protected void addBinaryInput(Column[] column, BitMap bitMap, int lastIndex) {
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
              if (bitMap != null && !bitMap.isMarked(curIndex)) {
                continue;
              }
              updateBinaryFirstValue(curPattern.getBinary(0), column[0].getLong(curIndex));
            }
          } else {
            for (int j = 0; j < curPatternCount; j++, curIndex++) {
              if (bitMap != null && !bitMap.isMarked(curIndex)) {
                continue;
              }
              if (!curPattern.isNull(j)) {
                updateBinaryFirstValue(curPattern.getBinary(j), column[0].getLong(curIndex));
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
        updateBinaryFirstValue(column[1].getBinary(i), column[0].getLong(i));
      }
    }
  }
}
