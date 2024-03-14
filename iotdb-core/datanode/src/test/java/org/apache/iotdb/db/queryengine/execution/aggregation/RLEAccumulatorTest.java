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

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.TimeStatistics;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.DoubleColumn;
import org.apache.iotdb.tsfile.read.common.block.column.DoubleColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.LongColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.RLEPatternColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BytesUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class RLEAccumulatorTest {

  private TsBlock rawData;
  private Statistics statistics;
  private double finalValueSum;

  @Before
  public void setUp() {
    initInputTsBlock();
  }

  private double[] generateArrayDouble(int positionCount, double index) {
    double[] doubles = new double[positionCount];
    for (int i = 0; i < positionCount; i++) {
      doubles[i] = ((double) index + i);
      finalValueSum += doubles[i];
    }
    return doubles;
  }

  private void initInputTsBlock() {
    finalValueSum = 0;
    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.DOUBLE);
    TsBlockBuilder tsBlockBuilder =
        new TsBlockBuilder(Collections.singletonList(TSDataType.RLEPATTERN));
    TimeColumnBuilder timeColumnBuilder = tsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();

    int index = 0;
    for (int j = 0; j < 100; j++) {
      int positionCount = 10;
      RLEPatternColumn column;
      if (j % 3 != 0) {
        column =
            new RLEPatternColumn(
                new DoubleColumn(1, Optional.empty(), new double[] {index}), positionCount, 0);
        finalValueSum += index * positionCount;
      } else {
        column =
            new RLEPatternColumn(
                new DoubleColumn(
                    positionCount, Optional.empty(), generateArrayDouble(positionCount, index)),
                positionCount,
                1);
      }

      for (int i = 0; i < positionCount; i++, index++) {
        timeColumnBuilder.writeLong(index);
      }

      (columnBuilders[0]).writeObject(column);
      tsBlockBuilder.declarePositions(positionCount);
    }
    rawData = tsBlockBuilder.build();

    statistics = Statistics.getStatsByType(TSDataType.DOUBLE);
    statistics.update(1000L, 1000d);
  }

  public Column[] getTimeAndValueColumn(int columnIndex) {
    Column[] columns = new Column[2];
    columns[0] = rawData.getTimeColumn();
    columns[1] = rawData.getColumn(columnIndex);
    return columns;
  }

  @Test
  public void avgAccumulatorTest() {
    Accumulator avgAccumulator =
        AccumulatorFactory.createAccumulator(
            TAggregationType.AVG,
            TSDataType.DOUBLE,
            Collections.emptyList(),
            Collections.emptyMap(),
            true);
    Assert.assertEquals(TSDataType.INT64, avgAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.DOUBLE, avgAccumulator.getIntermediateType()[1]);
    Assert.assertEquals(TSDataType.DOUBLE, avgAccumulator.getFinalType());
    // check returning null while no data
    ColumnBuilder[] intermediateResult = new ColumnBuilder[2];
    intermediateResult[0] = new LongColumnBuilder(null, 1);
    intermediateResult[1] = new DoubleColumnBuilder(null, 1);
    avgAccumulator.outputIntermediate(intermediateResult);
    Assert.assertTrue(intermediateResult[0].build().isNull(0));
    Assert.assertTrue(intermediateResult[1].build().isNull(0));
    ColumnBuilder finalResult = new DoubleColumnBuilder(null, 1);
    avgAccumulator.outputFinal(finalResult);
    Assert.assertTrue(finalResult.build().isNull(0));

    Column[] timeAndValueColumn = getTimeAndValueColumn(0);
    avgAccumulator.addInput(timeAndValueColumn, null, rawData.getPositionCount() - 1);
    Assert.assertFalse(avgAccumulator.hasFinalResult());
    intermediateResult[0] = new LongColumnBuilder(null, 1);
    intermediateResult[1] = new DoubleColumnBuilder(null, 1);
    avgAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(1000, intermediateResult[0].build().getLong(0));
    Assert.assertEquals((double) finalValueSum, intermediateResult[1].build().getDouble(0), 0.001);

    // add intermediate result as input
    avgAccumulator.addIntermediate(
        new Column[] {intermediateResult[0].build(), intermediateResult[1].build()});
    finalResult = new DoubleColumnBuilder(null, 1);
    avgAccumulator.outputFinal(finalResult);
    Assert.assertEquals(finalValueSum / 1000, finalResult.build().getDouble(0), 0.001);

    // test remove partial result interface
    avgAccumulator.removeIntermediate(
        new Column[] {intermediateResult[0].build(), intermediateResult[1].build()});
    finalResult = new DoubleColumnBuilder(null, 1);
    avgAccumulator.outputFinal(finalResult);
    Assert.assertEquals(finalValueSum / 1000, finalResult.build().getDouble(0), 0.001);

    avgAccumulator.reset();
    avgAccumulator.addStatistics(statistics);
    finalResult = new DoubleColumnBuilder(null, 1);
    avgAccumulator.outputFinal(finalResult);
    Assert.assertEquals(1000d, finalResult.build().getDouble(0), 0.001);
  }

  @Test
  public void countAccumulatorTest() {
    Accumulator countAccumulator =
        AccumulatorFactory.createAccumulator(
            TAggregationType.COUNT,
            TSDataType.DOUBLE,
            Collections.emptyList(),
            Collections.emptyMap(),
            true);
    Assert.assertEquals(TSDataType.INT64, countAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.INT64, countAccumulator.getFinalType());
    // check returning null while no data
    ColumnBuilder[] intermediateResult = new ColumnBuilder[1];
    intermediateResult[0] = new LongColumnBuilder(null, 1);
    countAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(0, intermediateResult[0].build().getLong(0));
    ColumnBuilder finalResult = new LongColumnBuilder(null, 1);
    countAccumulator.outputFinal(finalResult);
    Assert.assertEquals(0, finalResult.build().getLong(0));

    Column[] timeAndValueColumn = getTimeAndValueColumn(0);
    countAccumulator.addInput(timeAndValueColumn, null, rawData.getPositionCount() - 1);
    Assert.assertFalse(countAccumulator.hasFinalResult());
    intermediateResult[0] = new LongColumnBuilder(null, 1);
    countAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(1000, intermediateResult[0].build().getLong(0));

    // add intermediate result as input
    countAccumulator.addIntermediate(new Column[] {intermediateResult[0].build()});
    finalResult = new LongColumnBuilder(null, 1);
    countAccumulator.outputFinal(finalResult);
    Assert.assertEquals(2000, finalResult.build().getLong(0));

    // test remove partial result interface
    countAccumulator.removeIntermediate(new Column[] {intermediateResult[0].build()});
    finalResult = new LongColumnBuilder(null, 1);
    countAccumulator.outputFinal(finalResult);
    Assert.assertEquals(1000, finalResult.build().getLong(0), 0.001);

    countAccumulator.reset();
    countAccumulator.addStatistics(statistics);
    finalResult = new LongColumnBuilder(null, 1);
    countAccumulator.outputFinal(finalResult);
    Assert.assertEquals(1, finalResult.build().getLong(0));
  }

  @Test
  public void countTimeAccumulatorTest() {
    Accumulator countTimeAccumulator =
        AccumulatorFactory.createAccumulator(
            TAggregationType.COUNT_TIME,
            TSDataType.DOUBLE,
            Collections.emptyList(),
            Collections.emptyMap(),
            true);
    Assert.assertEquals(TSDataType.INT64, countTimeAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.INT64, countTimeAccumulator.getFinalType());
    // check returning null while no data
    ColumnBuilder[] intermediateResult = new ColumnBuilder[1];
    intermediateResult[0] = new LongColumnBuilder(null, 1);
    countTimeAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(0, intermediateResult[0].build().getLong(0));
    ColumnBuilder finalResult = new LongColumnBuilder(null, 1);
    countTimeAccumulator.outputFinal(finalResult);
    Assert.assertEquals(0, finalResult.build().getLong(0));

    Column[] timeAndValueColumn = getTimeAndValueColumn(0);
    timeAndValueColumn[1] = timeAndValueColumn[0];
    countTimeAccumulator.addInput(timeAndValueColumn, null, rawData.getPositionCount() - 1);
    Assert.assertFalse(countTimeAccumulator.hasFinalResult());
    intermediateResult[0] = new LongColumnBuilder(null, 1);
    countTimeAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(1000, intermediateResult[0].build().getLong(0));

    // add intermediate result as input
    countTimeAccumulator.addIntermediate(new Column[] {intermediateResult[0].build()});
    finalResult = new LongColumnBuilder(null, 1);
    countTimeAccumulator.outputFinal(finalResult);
    Assert.assertEquals(2000, finalResult.build().getLong(0));

    countTimeAccumulator.reset();
    TimeStatistics timeStatistics = new TimeStatistics();
    timeStatistics.update(1000L);
    countTimeAccumulator.addStatistics(timeStatistics);
    finalResult = new LongColumnBuilder(null, 1);
    countTimeAccumulator.outputFinal(finalResult);
    Assert.assertEquals(1, finalResult.build().getLong(0));
  }

  @Test
  public void extremeAccumulatorTest() {
    Accumulator extremeAccumulator =
        AccumulatorFactory.createAccumulator(
            TAggregationType.EXTREME,
            TSDataType.DOUBLE,
            Collections.emptyList(),
            Collections.emptyMap(),
            true);
    Assert.assertEquals(TSDataType.DOUBLE, extremeAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.DOUBLE, extremeAccumulator.getFinalType());
    // check returning null while no data
    ColumnBuilder[] intermediateResult = new ColumnBuilder[1];
    intermediateResult[0] = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputIntermediate(intermediateResult);
    Assert.assertTrue(intermediateResult[0].build().isNull(0));
    ColumnBuilder finalResult = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputFinal(finalResult);
    Assert.assertTrue(finalResult.build().isNull(0));

    Column[] timeAndValueColumn = getTimeAndValueColumn(0);
    extremeAccumulator.addInput(timeAndValueColumn, null, rawData.getPositionCount() - 1);
    Assert.assertFalse(extremeAccumulator.hasFinalResult());
    intermediateResult[0] = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(999d, intermediateResult[0].build().getDouble(0), 0.001);

    // add intermediate result as input
    extremeAccumulator.addIntermediate(new Column[] {intermediateResult[0].build()});
    finalResult = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputFinal(finalResult);
    Assert.assertEquals(999d, finalResult.build().getDouble(0), 0.001);

    extremeAccumulator.reset();
    extremeAccumulator.addStatistics(statistics);
    finalResult = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputFinal(finalResult);
    Assert.assertEquals(1000d, finalResult.build().getDouble(0), 0.001);
  }

  @Test
  public void firstValueAccumulatorTest() {
    Accumulator firstValueAccumulator =
        AccumulatorFactory.createAccumulator(
            TAggregationType.FIRST_VALUE,
            TSDataType.DOUBLE,
            Collections.emptyList(),
            Collections.emptyMap(),
            true);
    Assert.assertEquals(TSDataType.DOUBLE, firstValueAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.INT64, firstValueAccumulator.getIntermediateType()[1]);
    Assert.assertEquals(TSDataType.DOUBLE, firstValueAccumulator.getFinalType());
    // check returning null while no data
    ColumnBuilder[] intermediateResult = new ColumnBuilder[2];
    intermediateResult[0] = new DoubleColumnBuilder(null, 1);
    intermediateResult[1] = new LongColumnBuilder(null, 1);
    firstValueAccumulator.outputIntermediate(intermediateResult);
    Assert.assertTrue(intermediateResult[0].build().isNull(0));
    Assert.assertTrue(intermediateResult[1].build().isNull(0));
    ColumnBuilder finalResult = new DoubleColumnBuilder(null, 1);
    firstValueAccumulator.outputFinal(finalResult);
    Assert.assertTrue(finalResult.build().isNull(0));

    Column[] timeAndValueColumn = getTimeAndValueColumn(0);
    firstValueAccumulator.addInput(timeAndValueColumn, null, rawData.getPositionCount() - 1);
    Assert.assertTrue(firstValueAccumulator.hasFinalResult());
    intermediateResult[0] = new DoubleColumnBuilder(null, 1);
    intermediateResult[1] = new LongColumnBuilder(null, 1);
    firstValueAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(0d, intermediateResult[0].build().getDouble(0), 0.001);
    Assert.assertEquals(0L, intermediateResult[1].build().getLong(0));

    // add intermediate result as input
    firstValueAccumulator.addIntermediate(
        new Column[] {intermediateResult[0].build(), intermediateResult[1].build()});
    finalResult = new DoubleColumnBuilder(null, 1);
    firstValueAccumulator.outputFinal(finalResult);
    Assert.assertEquals(0L, finalResult.build().getDouble(0), 0.001);

    firstValueAccumulator.reset();
    firstValueAccumulator.addStatistics(statistics);
    finalResult = new DoubleColumnBuilder(null, 1);
    firstValueAccumulator.outputFinal(finalResult);
    Assert.assertEquals(1000d, finalResult.build().getDouble(0), 0.001);
  }

  @Test
  public void lastValueAccumulatorTest() {
    Accumulator lastValueAccumulator =
        AccumulatorFactory.createAccumulator(
            TAggregationType.LAST_VALUE,
            TSDataType.DOUBLE,
            Collections.emptyList(),
            Collections.emptyMap(),
            true);
    Assert.assertEquals(TSDataType.DOUBLE, lastValueAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.INT64, lastValueAccumulator.getIntermediateType()[1]);
    Assert.assertEquals(TSDataType.DOUBLE, lastValueAccumulator.getFinalType());
    // check returning null while no data
    ColumnBuilder[] intermediateResult = new ColumnBuilder[2];
    intermediateResult[0] = new DoubleColumnBuilder(null, 1);
    intermediateResult[1] = new LongColumnBuilder(null, 1);
    lastValueAccumulator.outputIntermediate(intermediateResult);
    Assert.assertTrue(intermediateResult[0].build().isNull(0));
    Assert.assertTrue(intermediateResult[1].build().isNull(0));
    ColumnBuilder finalResult = new DoubleColumnBuilder(null, 1);
    lastValueAccumulator.outputFinal(finalResult);
    Assert.assertTrue(finalResult.build().isNull(0));

    Column[] timeAndValueColumn = getTimeAndValueColumn(0);
    lastValueAccumulator.addInput(timeAndValueColumn, null, rawData.getPositionCount() - 1);
    intermediateResult[0] = new DoubleColumnBuilder(null, 1);
    intermediateResult[1] = new LongColumnBuilder(null, 1);
    lastValueAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(999d, intermediateResult[0].build().getDouble(0), 0.001);
    Assert.assertEquals(999L, intermediateResult[1].build().getLong(0));

    // add intermediate result as input
    lastValueAccumulator.addIntermediate(
        new Column[] {intermediateResult[0].build(), intermediateResult[1].build()});
    finalResult = new DoubleColumnBuilder(null, 1);
    lastValueAccumulator.outputFinal(finalResult);
    Assert.assertEquals(999L, finalResult.build().getDouble(0), 0.001);

    lastValueAccumulator.reset();
    lastValueAccumulator.addStatistics(statistics);
    finalResult = new DoubleColumnBuilder(null, 1);
    lastValueAccumulator.outputFinal(finalResult);
    Assert.assertEquals(1000d, finalResult.build().getDouble(0), 0.001);
  }

  @Test
  public void maxTimeAccumulatorTest() {
    Accumulator maxTimeAccumulator =
        AccumulatorFactory.createAccumulator(
            TAggregationType.MAX_TIME,
            TSDataType.DOUBLE,
            Collections.emptyList(),
            Collections.emptyMap(),
            true);
    Assert.assertEquals(TSDataType.INT64, maxTimeAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.INT64, maxTimeAccumulator.getFinalType());
    // check returning null while no data
    ColumnBuilder[] intermediateResult = new ColumnBuilder[1];
    intermediateResult[0] = new LongColumnBuilder(null, 1);
    maxTimeAccumulator.outputIntermediate(intermediateResult);
    Assert.assertTrue(intermediateResult[0].build().isNull(0));
    ColumnBuilder finalResult = new LongColumnBuilder(null, 1);
    maxTimeAccumulator.outputFinal(finalResult);
    Assert.assertTrue(finalResult.build().isNull(0));

    Column[] timeAndValueColumn = getTimeAndValueColumn(0);
    maxTimeAccumulator.addInput(timeAndValueColumn, null, rawData.getPositionCount() - 1);
    Assert.assertFalse(maxTimeAccumulator.hasFinalResult());
    intermediateResult[0] = new LongColumnBuilder(null, 1);
    maxTimeAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(999, intermediateResult[0].build().getLong(0));

    // add intermediate result as input
    maxTimeAccumulator.addIntermediate(new Column[] {intermediateResult[0].build()});
    finalResult = new LongColumnBuilder(null, 1);
    maxTimeAccumulator.outputFinal(finalResult);
    Assert.assertEquals(999, finalResult.build().getLong(0));

    maxTimeAccumulator.reset();
    maxTimeAccumulator.addStatistics(statistics);
    finalResult = new LongColumnBuilder(null, 1);
    maxTimeAccumulator.outputFinal(finalResult);
    Assert.assertEquals(1000, finalResult.build().getLong(0));
  }

  @Test
  public void minTimeAccumulatorTest() {
    Accumulator minTimeAccumulator =
        AccumulatorFactory.createAccumulator(
            TAggregationType.MIN_TIME,
            TSDataType.DOUBLE,
            Collections.emptyList(),
            Collections.emptyMap(),
            true);
    Assert.assertEquals(TSDataType.INT64, minTimeAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.INT64, minTimeAccumulator.getFinalType());
    // check returning null while no data
    ColumnBuilder[] intermediateResult = new ColumnBuilder[1];
    intermediateResult[0] = new LongColumnBuilder(null, 1);
    minTimeAccumulator.outputIntermediate(intermediateResult);
    Assert.assertTrue(intermediateResult[0].build().isNull(0));
    ColumnBuilder finalResult = new LongColumnBuilder(null, 1);
    minTimeAccumulator.outputFinal(finalResult);
    Assert.assertTrue(finalResult.build().isNull(0));

    Column[] timeAndValueColumn = getTimeAndValueColumn(0);
    minTimeAccumulator.addInput(timeAndValueColumn, null, rawData.getPositionCount() - 1);
    Assert.assertTrue(minTimeAccumulator.hasFinalResult());
    intermediateResult[0] = new LongColumnBuilder(null, 1);
    minTimeAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(0, intermediateResult[0].build().getLong(0));

    // add intermediate result as input
    minTimeAccumulator.addIntermediate(new Column[] {intermediateResult[0].build()});
    finalResult = new LongColumnBuilder(null, 1);
    minTimeAccumulator.outputFinal(finalResult);
    Assert.assertEquals(0, finalResult.build().getLong(0));

    minTimeAccumulator.reset();
    minTimeAccumulator.addStatistics(statistics);
    finalResult = new LongColumnBuilder(null, 1);
    minTimeAccumulator.outputFinal(finalResult);
    Assert.assertEquals(1000, finalResult.build().getLong(0));
  }

  @Test
  public void maxValueAccumulatorTest() {
    Accumulator extremeAccumulator =
        AccumulatorFactory.createAccumulator(
            TAggregationType.MAX_VALUE,
            TSDataType.DOUBLE,
            Collections.emptyList(),
            Collections.emptyMap(),
            true);
    Assert.assertEquals(TSDataType.DOUBLE, extremeAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.DOUBLE, extremeAccumulator.getFinalType());
    // check returning null while no data
    ColumnBuilder[] intermediateResult = new ColumnBuilder[1];
    intermediateResult[0] = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputIntermediate(intermediateResult);
    Assert.assertTrue(intermediateResult[0].build().isNull(0));
    ColumnBuilder finalResult = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputFinal(finalResult);
    Assert.assertTrue(finalResult.build().isNull(0));

    Column[] timeAndValueColumn = getTimeAndValueColumn(0);
    extremeAccumulator.addInput(timeAndValueColumn, null, rawData.getPositionCount() - 1);
    Assert.assertFalse(extremeAccumulator.hasFinalResult());
    intermediateResult[0] = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(999d, intermediateResult[0].build().getDouble(0), 0.001);

    // add intermediate result as input
    extremeAccumulator.addIntermediate(new Column[] {intermediateResult[0].build()});
    finalResult = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputFinal(finalResult);
    Assert.assertEquals(999d, finalResult.build().getDouble(0), 0.001);

    extremeAccumulator.reset();
    extremeAccumulator.addStatistics(statistics);
    finalResult = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputFinal(finalResult);
    Assert.assertEquals(1000d, finalResult.build().getDouble(0), 0.001);
  }

  @Test
  public void minValueAccumulatorTest() {
    Accumulator extremeAccumulator =
        AccumulatorFactory.createAccumulator(
            TAggregationType.MIN_VALUE,
            TSDataType.DOUBLE,
            Collections.emptyList(),
            Collections.emptyMap(),
            true);
    Assert.assertEquals(TSDataType.DOUBLE, extremeAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.DOUBLE, extremeAccumulator.getFinalType());
    // check returning null while no data
    ColumnBuilder[] intermediateResult = new ColumnBuilder[1];
    intermediateResult[0] = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputIntermediate(intermediateResult);
    Assert.assertTrue(intermediateResult[0].build().isNull(0));
    ColumnBuilder finalResult = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputFinal(finalResult);
    Assert.assertTrue(finalResult.build().isNull(0));

    Column[] timeAndValueColumn = getTimeAndValueColumn(0);
    extremeAccumulator.addInput(timeAndValueColumn, null, rawData.getPositionCount() - 1);
    Assert.assertFalse(extremeAccumulator.hasFinalResult());
    intermediateResult[0] = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(0d, intermediateResult[0].build().getDouble(0), 0.001);

    // add intermediate result as input
    extremeAccumulator.addIntermediate(new Column[] {intermediateResult[0].build()});
    finalResult = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputFinal(finalResult);
    Assert.assertEquals(0d, finalResult.build().getDouble(0), 0.001);

    extremeAccumulator.reset();
    extremeAccumulator.addStatistics(statistics);
    finalResult = new DoubleColumnBuilder(null, 1);
    extremeAccumulator.outputFinal(finalResult);
    Assert.assertEquals(1000d, finalResult.build().getDouble(0), 0.001);
  }

  @Test
  public void sumAccumulatorTest() {
    Accumulator sumAccumulator =
        AccumulatorFactory.createAccumulator(
            TAggregationType.SUM,
            TSDataType.DOUBLE,
            Collections.emptyList(),
            Collections.emptyMap(),
            true);
    Assert.assertEquals(TSDataType.DOUBLE, sumAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.DOUBLE, sumAccumulator.getFinalType());
    // check returning null while no data
    ColumnBuilder[] intermediateResult = new ColumnBuilder[1];
    intermediateResult[0] = new DoubleColumnBuilder(null, 1);
    sumAccumulator.outputIntermediate(intermediateResult);
    Assert.assertTrue(intermediateResult[0].build().isNull(0));
    ColumnBuilder finalResult = new DoubleColumnBuilder(null, 1);
    sumAccumulator.outputFinal(finalResult);
    Assert.assertTrue(finalResult.build().isNull(0));

    Column[] timeAndValueColumn = getTimeAndValueColumn(0);
    sumAccumulator.addInput(timeAndValueColumn, null, rawData.getPositionCount() - 1);
    Assert.assertFalse(sumAccumulator.hasFinalResult());
    intermediateResult[0] = new DoubleColumnBuilder(null, 1);
    sumAccumulator.outputIntermediate(intermediateResult);
    Assert.assertEquals(finalValueSum, intermediateResult[0].build().getDouble(0), 0.001);

    // add intermediate result as input
    sumAccumulator.addIntermediate(new Column[] {intermediateResult[0].build()});
    finalResult = new DoubleColumnBuilder(null, 1);
    sumAccumulator.outputFinal(finalResult);
    Assert.assertEquals(finalValueSum * 2, finalResult.build().getDouble(0), 0.001);

    // test remove partial result interface
    sumAccumulator.removeIntermediate(new Column[] {intermediateResult[0].build()});
    finalResult = new DoubleColumnBuilder(null, 1);
    sumAccumulator.outputFinal(finalResult);
    Assert.assertEquals(finalValueSum, finalResult.build().getDouble(0), 0.001);

    sumAccumulator.reset();
    sumAccumulator.addStatistics(statistics);
    finalResult = new DoubleColumnBuilder(null, 1);
    sumAccumulator.outputFinal(finalResult);
    Assert.assertEquals(1000d, finalResult.build().getDouble(0), 0.001);
  }

  @Test
  public void stddevAccumulatorTest() {
    Accumulator stddevAccumulator =
        AccumulatorFactory.createAccumulator(
            TAggregationType.STDDEV,
            TSDataType.DOUBLE,
            Collections.emptyList(),
            Collections.emptyMap(),
            true);
    // check intermediate type and final type
    Assert.assertEquals(TSDataType.TEXT, stddevAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.DOUBLE, stddevAccumulator.getFinalType());
    // check returning null when no data
    ColumnBuilder[] intermediateResult = new ColumnBuilder[1];
    intermediateResult[0] = new BinaryColumnBuilder(null, 1);
    stddevAccumulator.outputIntermediate(intermediateResult);
    Assert.assertTrue(intermediateResult[0].build().isNull(0));
    ColumnBuilder finalResult = new DoubleColumnBuilder(null, 1);
    stddevAccumulator.outputFinal(finalResult);
    Assert.assertTrue(finalResult.build().isNull(0));
    // check returning null when no enough data
    intermediateResult[0].writeBinary(new Binary(new byte[0]));
    stddevAccumulator.outputIntermediate(intermediateResult);
    Assert.assertTrue(intermediateResult[0].build().isNull(0));
    finalResult = new DoubleColumnBuilder(null, 1);
    stddevAccumulator.outputFinal(finalResult);
    Assert.assertTrue(finalResult.build().isNull(0));

    Column[] timeAndValueColumn = getTimeAndValueColumn(0);
    stddevAccumulator.addInput(timeAndValueColumn, null, rawData.getPositionCount() - 1);
    Assert.assertFalse(stddevAccumulator.hasFinalResult());
    intermediateResult[0] = new BinaryColumnBuilder(null, 1);
    stddevAccumulator.outputIntermediate(intermediateResult);
    byte[] result = intermediateResult[0].build().getBinary(0).getValues();
    Assert.assertEquals(1000, BytesUtils.bytesToLong(result, Long.BYTES));
    Assert.assertEquals(496.52999, BytesUtils.bytesToDouble(result, Long.BYTES), 0.001);
    Assert.assertEquals(
      8.333234910000008E7, BytesUtils.bytesToDouble(result, (Long.BYTES + Double.BYTES)), 0.001);

    stddevAccumulator.addIntermediate(
        new Column[] {
          intermediateResult[0].build(),
        });
    finalResult = new DoubleColumnBuilder(null, 1);
    stddevAccumulator.outputFinal(finalResult);
    Assert.assertEquals(288.7456252795168, finalResult.build().getDouble(0), 0.001);

    // test remove partial result interface
    stddevAccumulator.removeIntermediate(new Column[] {intermediateResult[0].build()});
    finalResult = new DoubleColumnBuilder(null, 1);
    stddevAccumulator.outputFinal(finalResult);
    Assert.assertEquals(288.81787490538903, finalResult.build().getDouble(0), 0.001);
  }

  @Test
  public void stddevPopAccumulatorTest() {
    Accumulator stddevPopAccumulator =
        AccumulatorFactory.createAccumulator(
            TAggregationType.STDDEV_POP,
            TSDataType.DOUBLE,
            Collections.emptyList(),
            Collections.emptyMap(),
            true);
    // check intermediate type and final type
    Assert.assertEquals(TSDataType.TEXT, stddevPopAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.DOUBLE, stddevPopAccumulator.getFinalType());
    // check returning null when no data
    ColumnBuilder[] intermediateResult = new ColumnBuilder[1];
    intermediateResult[0] = new BinaryColumnBuilder(null, 1);
    stddevPopAccumulator.outputIntermediate(intermediateResult);
    Assert.assertTrue(intermediateResult[0].build().isNull(0));
    ColumnBuilder finalResult = new DoubleColumnBuilder(null, 1);
    stddevPopAccumulator.outputFinal(finalResult);
    Assert.assertTrue(finalResult.build().isNull(0));

    Column[] timeAndValueColumn = getTimeAndValueColumn(0);
    stddevPopAccumulator.addInput(timeAndValueColumn, null, rawData.getPositionCount() - 1);
    Assert.assertFalse(stddevPopAccumulator.hasFinalResult());
    intermediateResult[0] = new BinaryColumnBuilder(null, 1);
    stddevPopAccumulator.outputIntermediate(intermediateResult);
    byte[] result = intermediateResult[0].build().getBinary(0).getValues();
    Assert.assertEquals(1000, BytesUtils.bytesToLong(result, Long.BYTES));
    Assert.assertEquals(49.50, BytesUtils.bytesToDouble(result, Long.BYTES), 0.001);
    Assert.assertEquals(
        83325, BytesUtils.bytesToDouble(result, (Long.BYTES + Double.BYTES)), 0.001);

    stddevPopAccumulator.addIntermediate(
        new Column[] {
          intermediateResult[0].build(),
        });
    finalResult = new DoubleColumnBuilder(null, 1);
    stddevPopAccumulator.outputFinal(finalResult);
    Assert.assertEquals(28.866, finalResult.build().getDouble(0), 0.001);

    // test remove partial result interface
    stddevPopAccumulator.removeIntermediate(new Column[] {intermediateResult[0].build()});
    finalResult = new DoubleColumnBuilder(null, 1);
    stddevPopAccumulator.outputFinal(finalResult);
    Assert.assertEquals(28.86607004772212, finalResult.build().getDouble(0), 0.001);
  }

  @Test
  public void stddevSampAccumulatorTest() {
    Accumulator stddevSampAccumulator =
        AccumulatorFactory.createAccumulator(
            TAggregationType.STDDEV_SAMP,
            TSDataType.DOUBLE,
            Collections.emptyList(),
            Collections.emptyMap(),
            true);
    // check intermediate type and final type
    Assert.assertEquals(TSDataType.TEXT, stddevSampAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.DOUBLE, stddevSampAccumulator.getFinalType());
    // check returning null when no data
    ColumnBuilder[] intermediateResult = new ColumnBuilder[1];
    intermediateResult[0] = new BinaryColumnBuilder(null, 1);
    stddevSampAccumulator.outputIntermediate(intermediateResult);
    Assert.assertTrue(intermediateResult[0].build().isNull(0));
    ColumnBuilder finalResult = new DoubleColumnBuilder(null, 1);
    stddevSampAccumulator.outputFinal(finalResult);
    Assert.assertTrue(finalResult.build().isNull(0));
    // check returning null when no enough data
    intermediateResult[0].writeBinary(new Binary(new byte[0]));
    stddevSampAccumulator.outputIntermediate(intermediateResult);
    Assert.assertTrue(intermediateResult[0].build().isNull(0));
    finalResult = new DoubleColumnBuilder(null, 1);
    stddevSampAccumulator.outputFinal(finalResult);
    Assert.assertTrue(finalResult.build().isNull(0));

    Column[] timeAndValueColumn = getTimeAndValueColumn(0);
    stddevSampAccumulator.addInput(timeAndValueColumn, null, rawData.getPositionCount() - 1);
    Assert.assertFalse(stddevSampAccumulator.hasFinalResult());
    intermediateResult[0] = new BinaryColumnBuilder(null, 1);
    stddevSampAccumulator.outputIntermediate(intermediateResult);
    byte[] result = intermediateResult[0].build().getBinary(0).getValues();
    Assert.assertEquals(100, BytesUtils.bytesToLong(result, Long.BYTES));
    Assert.assertEquals(49.50, BytesUtils.bytesToDouble(result, Long.BYTES), 0.001);
    Assert.assertEquals(
        83325, BytesUtils.bytesToDouble(result, (Long.BYTES + Double.BYTES)), 0.001);

    stddevSampAccumulator.addIntermediate(
        new Column[] {
          intermediateResult[0].build(),
        });
    finalResult = new DoubleColumnBuilder(null, 1);
    stddevSampAccumulator.outputFinal(finalResult);
    Assert.assertEquals(28.938, finalResult.build().getDouble(0), 0.001);

    // test remove partial result interface
    stddevSampAccumulator.removeIntermediate(new Column[] {intermediateResult[0].build()});
    finalResult = new DoubleColumnBuilder(null, 1);
    stddevSampAccumulator.outputFinal(finalResult);
    Assert.assertEquals(29.011491975882016, finalResult.build().getDouble(0), 0.001);
  }

  @Test
  public void varianceAccumulatorTest() {
    Accumulator varianceAccumulator =
        AccumulatorFactory.createAccumulator(
            TAggregationType.VARIANCE,
            TSDataType.DOUBLE,
            Collections.emptyList(),
            Collections.emptyMap(),
            true);
    // check intermediate type and final type
    Assert.assertEquals(TSDataType.TEXT, varianceAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.DOUBLE, varianceAccumulator.getFinalType());
    // check returning null when no data
    ColumnBuilder[] intermediateResult = new ColumnBuilder[1];
    intermediateResult[0] = new BinaryColumnBuilder(null, 1);
    varianceAccumulator.outputIntermediate(intermediateResult);
    Assert.assertTrue(intermediateResult[0].build().isNull(0));
    ColumnBuilder finalResult = new DoubleColumnBuilder(null, 1);
    varianceAccumulator.outputFinal(finalResult);
    Assert.assertTrue(finalResult.build().isNull(0));
    // check returning null when no enough data
    intermediateResult[0].writeBinary(new Binary(new byte[0]));
    varianceAccumulator.outputIntermediate(intermediateResult);
    Assert.assertTrue(intermediateResult[0].build().isNull(0));
    finalResult = new DoubleColumnBuilder(null, 1);
    varianceAccumulator.outputFinal(finalResult);
    Assert.assertTrue(finalResult.build().isNull(0));

    Column[] timeAndValueColumn = getTimeAndValueColumn(0);
    varianceAccumulator.addInput(timeAndValueColumn, null, rawData.getPositionCount() - 1);
    Assert.assertFalse(varianceAccumulator.hasFinalResult());
    intermediateResult[0] = new BinaryColumnBuilder(null, 1);
    varianceAccumulator.outputIntermediate(intermediateResult);
    byte[] result = intermediateResult[0].build().getBinary(0).getValues();
    Assert.assertEquals(100, BytesUtils.bytesToLong(result, Long.BYTES));
    Assert.assertEquals(49.50, BytesUtils.bytesToDouble(result, Long.BYTES), 0.001);
    Assert.assertEquals(
        83325, BytesUtils.bytesToDouble(result, (Long.BYTES + Double.BYTES)), 0.001);

    varianceAccumulator.addIntermediate(
        new Column[] {
          intermediateResult[0].build(),
        });
    finalResult = new DoubleColumnBuilder(null, 1);
    varianceAccumulator.outputFinal(finalResult);
    Assert.assertEquals(837.437, finalResult.build().getDouble(0), 0.001);

    // test remove partial result interface
    varianceAccumulator.removeIntermediate(new Column[] {intermediateResult[0].build()});
    finalResult = new DoubleColumnBuilder(null, 1);
    varianceAccumulator.outputFinal(finalResult);
    Assert.assertEquals(841.6666666666666, finalResult.build().getDouble(0), 0.001);
  }

  @Test
  public void varPopAccumulatorTest() {
    Accumulator varPopAccumulator =
        AccumulatorFactory.createAccumulator(
            TAggregationType.VAR_POP,
            TSDataType.DOUBLE,
            Collections.emptyList(),
            Collections.emptyMap(),
            true);
    // check intermediate type and final type
    Assert.assertEquals(TSDataType.TEXT, varPopAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.DOUBLE, varPopAccumulator.getFinalType());
    // check returning null when no data
    ColumnBuilder[] intermediateResult = new ColumnBuilder[1];
    intermediateResult[0] = new BinaryColumnBuilder(null, 1);
    varPopAccumulator.outputIntermediate(intermediateResult);
    Assert.assertTrue(intermediateResult[0].build().isNull(0));
    ColumnBuilder finalResult = new DoubleColumnBuilder(null, 1);
    varPopAccumulator.outputFinal(finalResult);
    Assert.assertTrue(finalResult.build().isNull(0));

    Column[] timeAndValueColumn = getTimeAndValueColumn(0);
    varPopAccumulator.addInput(timeAndValueColumn, null, rawData.getPositionCount() - 1);
    Assert.assertFalse(varPopAccumulator.hasFinalResult());
    intermediateResult[0] = new BinaryColumnBuilder(null, 1);
    varPopAccumulator.outputIntermediate(intermediateResult);
    byte[] result = intermediateResult[0].build().getBinary(0).getValues();
    Assert.assertEquals(100, BytesUtils.bytesToLong(result, Long.BYTES));
    Assert.assertEquals(49.50, BytesUtils.bytesToDouble(result, Long.BYTES), 0.001);
    Assert.assertEquals(
        83325, BytesUtils.bytesToDouble(result, (Long.BYTES + Double.BYTES)), 0.001);

    varPopAccumulator.addIntermediate(
        new Column[] {
          intermediateResult[0].build(),
        });
    finalResult = new DoubleColumnBuilder(null, 1);
    varPopAccumulator.outputFinal(finalResult);
    Assert.assertEquals(833.25, finalResult.build().getDouble(0), 0.001);

    // test remove partial result interface
    varPopAccumulator.removeIntermediate(new Column[] {intermediateResult[0].build()});
    finalResult = new DoubleColumnBuilder(null, 1);
    varPopAccumulator.outputFinal(finalResult);
    Assert.assertEquals(833.25, finalResult.build().getDouble(0), 0.001);
  }

  @Test
  public void varSampAccumulatorTest() {
    Accumulator varSampAccumulator =
        AccumulatorFactory.createAccumulator(
            TAggregationType.VAR_SAMP,
            TSDataType.DOUBLE,
            Collections.emptyList(),
            Collections.emptyMap(),
            true);
    // check intermediate type and final type
    Assert.assertEquals(TSDataType.TEXT, varSampAccumulator.getIntermediateType()[0]);
    Assert.assertEquals(TSDataType.DOUBLE, varSampAccumulator.getFinalType());
    // check returning null when no data
    ColumnBuilder[] intermediateResult = new ColumnBuilder[1];
    intermediateResult[0] = new BinaryColumnBuilder(null, 1);
    varSampAccumulator.outputIntermediate(intermediateResult);
    Assert.assertTrue(intermediateResult[0].build().isNull(0));
    ColumnBuilder finalResult = new DoubleColumnBuilder(null, 1);
    varSampAccumulator.outputFinal(finalResult);
    Assert.assertTrue(finalResult.build().isNull(0));
    // check returning null when no enough data
    intermediateResult[0].writeBinary(new Binary(new byte[0]));
    varSampAccumulator.outputIntermediate(intermediateResult);
    Assert.assertTrue(intermediateResult[0].build().isNull(0));
    finalResult = new DoubleColumnBuilder(null, 1);
    varSampAccumulator.outputFinal(finalResult);
    Assert.assertTrue(finalResult.build().isNull(0));

    Column[] timeAndValueColumn = getTimeAndValueColumn(0);
    varSampAccumulator.addInput(timeAndValueColumn, null, rawData.getPositionCount() - 1);
    Assert.assertFalse(varSampAccumulator.hasFinalResult());
    intermediateResult[0] = new BinaryColumnBuilder(null, 1);
    varSampAccumulator.outputIntermediate(intermediateResult);
    byte[] result = intermediateResult[0].build().getBinary(0).getValues();
    Assert.assertEquals(100, BytesUtils.bytesToLong(result, Long.BYTES));
    Assert.assertEquals(49.50, BytesUtils.bytesToDouble(result, Long.BYTES), 0.001);
    Assert.assertEquals(
        83325, BytesUtils.bytesToDouble(result, (Long.BYTES + Double.BYTES)), 0.001);

    varSampAccumulator.addIntermediate(
        new Column[] {
          intermediateResult[0].build(),
        });
    finalResult = new DoubleColumnBuilder(null, 1);
    varSampAccumulator.outputFinal(finalResult);
    Assert.assertEquals(837.437, finalResult.build().getDouble(0), 0.001);

    // test remove partial result interface
    varSampAccumulator.removeIntermediate(new Column[] {intermediateResult[0].build()});
    finalResult = new DoubleColumnBuilder(null, 1);
    varSampAccumulator.outputFinal(finalResult);
    Assert.assertEquals(841.6666666666666, finalResult.build().getDouble(0), 0.001);
  }
}
