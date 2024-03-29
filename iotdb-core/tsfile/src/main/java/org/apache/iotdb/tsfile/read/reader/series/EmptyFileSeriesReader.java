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

package org.apache.iotdb.tsfile.read.reader.series;

import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.common.BatchData;

/** this is for those series which has no data points */
public class EmptyFileSeriesReader extends AbstractFileSeriesReader {
  BatchData data = new BatchData();

  public EmptyFileSeriesReader() {
    super(null, null, null);
  }

  @Override
  protected void initChunkReader(IChunkMetadata chunkMetaData) {
    // do nothing
  }

  @Override
  protected boolean chunkCanSkip(IChunkMetadata chunkMetaData) {
    return true;
  }

  @Override
  public boolean hasNextBatch() {
    return false;
  }

  @Override
  public BatchData nextBatch() {
    return data;
  }

  @Override
  public void close() {
    data = null;
  }
}
