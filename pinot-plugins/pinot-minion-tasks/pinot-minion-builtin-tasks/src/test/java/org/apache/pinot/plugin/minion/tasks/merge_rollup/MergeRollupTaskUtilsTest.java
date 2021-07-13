/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.plugin.minion.tasks.merge_rollup;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.core.segment.processing.framework.MergeType;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class MergeRollupTaskUtilsTest {
  private final String METRIC_COLUMN_A = "metricColA";
  private final String METRIC_COLUMN_B = "metricColB";
  private final String DAILY = "daily";
  private final String MONTHLY = "monthly";
  private Map<String, String> _mergeRollupTaskConfig;

  @BeforeClass
  public void setUp() {
    Map<String, String> mergeRollupTaskConfig = new HashMap<>();
    mergeRollupTaskConfig.put("metricColA.aggregationType", "sum");
    mergeRollupTaskConfig.put("metricColB.aggregationType", "max");
    mergeRollupTaskConfig.put("daily.mergeType", "concat");
    mergeRollupTaskConfig.put("daily.bufferTimePeriod", "2d");
    mergeRollupTaskConfig.put("daily.bucketTimePeriod", "1d");
    mergeRollupTaskConfig.put("daily.maxNumRecordsPerSegment", "1000000");
    mergeRollupTaskConfig.put("daily.maxNumRecordsPerTask", "5000000");
    mergeRollupTaskConfig.put("monthly.mergeType", "rollup");
    mergeRollupTaskConfig.put("monthly.bufferTimePeriod", "30d");
    mergeRollupTaskConfig.put("monthly.bucketTimePeriod", "30d");
    mergeRollupTaskConfig.put("monthly.maxNumRecordsPerSegment", "2000000");
    mergeRollupTaskConfig.put("monthly.maxNumRecordsPerTask", "5000000");
    _mergeRollupTaskConfig = mergeRollupTaskConfig;
  }

  @Test
  public void testGetRollupAggregationTypeMap() {
    Map<String, AggregationFunctionType> rollupAggregationTypeMap =
        MergeRollupTaskUtils.getRollupAggregationTypes(_mergeRollupTaskConfig);
    Assert.assertEquals(rollupAggregationTypeMap.size(), 2);
    Assert.assertTrue(rollupAggregationTypeMap.containsKey(METRIC_COLUMN_A));
    Assert.assertTrue(rollupAggregationTypeMap.containsKey(METRIC_COLUMN_B));
    Assert.assertEquals(rollupAggregationTypeMap.get(METRIC_COLUMN_A), AggregationFunctionType.SUM);
    Assert.assertEquals(rollupAggregationTypeMap.get(METRIC_COLUMN_B), AggregationFunctionType.MAX);
  }

  @Test
  public void testGetAllMergeProperties() {
    Map<String, MergeProperties> allMergeProperties =
        MergeRollupTaskUtils.getAllMergeProperties(_mergeRollupTaskConfig);
    Assert.assertEquals(allMergeProperties.size(), 2);
    Assert.assertTrue(allMergeProperties.containsKey(DAILY));
    Assert.assertTrue(allMergeProperties.containsKey(MONTHLY));

    MergeProperties dailyProperty = allMergeProperties.get(DAILY);
    Assert.assertEquals(dailyProperty.getMergeType(), MergeType.CONCAT.name());
    Assert.assertEquals(dailyProperty.getBufferTimePeriod(), "2d");
    Assert.assertEquals(dailyProperty.getBucketTimePeriod(), "1d");
    Assert.assertEquals(dailyProperty.getMaxNumRecordsPerSegment(), 1000000L);
    Assert.assertEquals(dailyProperty.getMaxNumRecordsPerTask(), 5000000L);

    MergeProperties monthlyProperty = allMergeProperties.get(MONTHLY);
    Assert.assertEquals(monthlyProperty.getMergeType(), MergeType.ROLLUP.name());
    Assert.assertEquals(monthlyProperty.getBufferTimePeriod(), "30d");
    Assert.assertEquals(monthlyProperty.getBucketTimePeriod(), "30d");
    Assert.assertEquals(monthlyProperty.getMaxNumRecordsPerSegment(), 2000000L);
    Assert.assertEquals(monthlyProperty.getMaxNumRecordsPerTask(), 5000000L);
  }
}
