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

package org.apache.iotdb.tsfile.read.common.block.column;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class RLEColumnEncoder implements ColumnEncoder {

  @Override
  public Column readColumn(ByteBuffer input, TSDataType dataType, int positionCount) {
    // Serialized data layout:
    //    +---------------+-----------------+-----------------------+
    //    | may have null | null indicators | values                |
    //    +---------------+-----------------+-----------------------+
    //    | byte          | list[byte]      | list[RLEPatternColum] |
    //    +---------------+-----------------+-----------------------+
    boolean[] nullIndicators = ColumnEncoder.deserializeNullIndicators(input, positionCount);
    RLEPatternColumnEncoder columnEncoder =
        (RLEPatternColumnEncoder) ColumnEncoderFactory.get(ColumnEncoding.RLE_PATTERN);
    RLEPatternColumn[] values = new RLEPatternColumn[positionCount];
    if (nullIndicators == null) {
      for (int i = 0; i < positionCount; i++) {
        values[i] = (RLEPatternColumn) columnEncoder.readColumn(input, dataType);
      }
    } else {
      for (int i = 0; i < positionCount; i++) {
        if (!nullIndicators[i]) {
          values[i] = (RLEPatternColumn) columnEncoder.readColumn(input, dataType);
        }
      }
    }
    return new RLEColumn(0, positionCount, nullIndicators, values);
  }

  @Override
  public void writeColumn(DataOutputStream output, Column column) throws IOException {
    if (!(column instanceof RLEColumn)) {
      throw new IllegalArgumentException("Unable to write column that not a RLEColumn");
    }
    RLEColumn RleColumn = (RLEColumn) column;

    ColumnEncoder.serializeNullIndicators(output, column);
    RLEPatternColumnEncoder columnEncoder =
        (RLEPatternColumnEncoder) ColumnEncoderFactory.get(ColumnEncoding.RLE_PATTERN);
    int positionCount = column.getPositionCount();
    if (column.mayHaveNull()) {
      for (int i = 0; i < positionCount; i++) {
        if (!column.isNull(i)) {
          columnEncoder.writeColumn(output, RleColumn.getRLEPattern(i));
        }
      }
    } else {
      for (int i = 0; i < positionCount; i++) {
        columnEncoder.writeColumn(output, RleColumn.getRLEPattern(i));
      }
    }
  }
}