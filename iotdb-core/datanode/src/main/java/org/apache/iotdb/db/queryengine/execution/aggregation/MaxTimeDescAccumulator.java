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

import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.RLEColumn;
import org.apache.iotdb.tsfile.read.common.block.column.RLEPatternColumn;
import org.apache.iotdb.tsfile.utils.BitMap;

public class MaxTimeDescAccumulator extends MaxTimeAccumulator {

  // Column should be like: | Time | Value |
  // Value is used to judge isNull()
  @Override
  public void addInput(Column[] column, BitMap bitMap, int lastIndex) {
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
              updateMaxTime(column[0].getLong(curIndex));
              return;
            }
          } else {
            for (int j = 0; j <= lastIndex; j++, curIndex++) {
              if (bitMap != null && !bitMap.isMarked(curIndex)) {
                continue;
              }
              if (!curPattern.isNull(j)) {
                updateMaxTime(column[0].getLong(curIndex));
                return;
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
        updateMaxTime(column[0].getLong(i));
        return;
      }
    }
  }

  @Override
  public boolean hasFinalResult() {
    return initResult;
  }
}
