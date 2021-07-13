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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.segment.processing.framework.MergeType;
import org.apache.pinot.pql.parsers.utils.Pair;
import org.apache.pinot.segment.spi.AggregationFunctionType;


public class MergeRollupTaskUtils {
  public static final long DEFAULT_MAX_NUM_RECORDS_PER_SEGMENT = 1_000_000L;
  public static final long DEFAULT_MAX_NUM_RECORDS_PER_TASK = 5_000_000L;

  //@formatter:off
  private static final String[] validMergeProperties = {
      MinionConstants.MergeRollupTask.MERGE_TYPE_KEY,
      MinionConstants.MergeRollupTask.BUFFER_TIME_PERIOD,
      MinionConstants.MergeRollupTask.BUCKET_TIME_PERIOD,
      MinionConstants.MergeRollupTask.ROUND_BUCKET_TIME_PERIOD,
      MinionConstants.MergeRollupTask.MAX_NUM_RECORDS_PER_SEGMENT,
      MinionConstants.MergeRollupTask.MAX_NUM_RECORDS_PER_TASK
  };

  private static final String[] validMergeType = {
      MergeType.CONCAT.name(),
      MergeType.ROLLUP.name()
  };
  //@formatter:on

  public static Map<String, AggregationFunctionType> getRollupAggregationTypes(Map<String, String> mergeRollupConfig) {
    Map<String, AggregationFunctionType> aggregationTypes = new HashMap<>();
    for (Map.Entry<String, String> entry : mergeRollupConfig.entrySet()) {
      if (entry.getKey().endsWith(MinionConstants.MergeRollupTask.AGGREGATION_TYPE_KEY_SUFFIX)) {
        aggregationTypes.put(getAggregateColumn(entry.getKey()),
            AggregationFunctionType.getAggregationFunctionType(entry.getValue()));
      }
    }
    return aggregationTypes;
  }

  public static Map<String, MergeProperties> getAllMergeProperties(Map<String, String> mergeRollupConfig) {
    Map<String, Map<String, String>> mergePropertiesMap = new HashMap<>();
    for (Map.Entry<String, String> entry : mergeRollupConfig.entrySet()) {
      if (isValidMergeProperty(entry.getKey(), entry.getValue())) {
        Pair<String, String> pair = getGranularityAndPropertyPair(entry.getKey());
        String granularity = pair.getFirst();
        String mergeProperty = pair.getSecond();
        mergePropertiesMap.putIfAbsent(granularity, new HashMap<>());
        mergePropertiesMap.get(granularity).put(mergeProperty, entry.getValue());
      }
    }

    Map<String, MergeProperties> allMergeProperties = new HashMap<>();
    for (Map.Entry<String, Map<String, String>> entry : mergePropertiesMap.entrySet()) {
      Map<String, String> properties = entry.getValue();
      long maxNumRecordsPerSegment =
          properties.get(MinionConstants.MergeRollupTask.MAX_NUM_RECORDS_PER_SEGMENT) != null ? Long.parseLong(
              properties.get(MinionConstants.MergeRollupTask.MAX_NUM_RECORDS_PER_SEGMENT))
              : DEFAULT_MAX_NUM_RECORDS_PER_SEGMENT;
      long maxNumRecordsPerTask =
          properties.get(MinionConstants.MergeRollupTask.MAX_NUM_RECORDS_PER_TASK) != null ? Long.parseLong(
              properties.get(MinionConstants.MergeRollupTask.MAX_NUM_RECORDS_PER_TASK))
              : DEFAULT_MAX_NUM_RECORDS_PER_TASK;
      MergeProperties mergeProperties =
          new MergeProperties(properties.get(MinionConstants.MergeRollupTask.MERGE_TYPE_KEY).toUpperCase(),
              properties.get(MinionConstants.MergeRollupTask.BUFFER_TIME_PERIOD),
              properties.get(MinionConstants.MergeRollupTask.BUCKET_TIME_PERIOD),
              properties.get(MinionConstants.MergeRollupTask.ROUND_BUCKET_TIME_PERIOD), maxNumRecordsPerSegment,
              maxNumRecordsPerTask);
      allMergeProperties.put(entry.getKey(), mergeProperties);
    }
    return allMergeProperties;
  }

  private static String getAggregateColumn(String rollupAggregateConfigKey) {
    return rollupAggregateConfigKey.split(MinionConstants.RealtimeToOfflineSegmentsTask.AGGREGATION_TYPE_KEY_SUFFIX)[0];
  }

  private static Pair<String, String> getGranularityAndPropertyPair(String mergePropertyConfigKey) {
    String[] components = StringUtils.split(mergePropertyConfigKey, MinionConstants.DOT_SEPARATOR);
    return new Pair<>(components[0], components[1]);
  }

  private static boolean isValidMergeProperty(String propertyKey, String propertyValue) {
    String[] components = StringUtils.split(propertyKey, MinionConstants.DOT_SEPARATOR);
    if (components.length != 2) {
      return false;
    }
    if (Arrays.stream(validMergeProperties).noneMatch(x -> x.equals(components[1]))) {
      return false;
    }
    if (components[1].equals(MinionConstants.MergeRollupTask.MERGE_TYPE_KEY) && !isValidMergeType(propertyValue)) {
      return false;
    }
    return true;
  }

  private static boolean isValidMergeType(String mergeType) {
    for (String validMergeType : validMergeType) {
      if (mergeType.toUpperCase().equals(validMergeType)) {
        return true;
      }
    }
    return false;
  }
}
