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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.lineage.LineageEntry;
import org.apache.pinot.common.lineage.LineageEntryState;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.lineage.SegmentLineageUtils;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.minion.MergeRollupTaskMetadata;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link MergeRollupTaskGenerator}
 */
public class MergeRollupTaskGeneratorTest {

  private static final String RAW_TABLE_NAME = "testTable";
  private static final String OFFLINE_TABLE_NAME = "testTable_OFFLINE";
  private static final String TIME_COLUMN_NAME = "millisSinceEpoch";
  private static final String DAILY = "daily";

  private TableConfig getOfflineTableConfig(Map<String, Map<String, String>> taskConfigsMap) {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN_NAME)
        .setTaskConfig(new TableTaskConfig(taskConfigsMap))
        .build();
  }

  /**
   * Tests for some config checks
   */
  @Test
  public void testGenerateTasksCheckConfigs() {
    ClusterInfoAccessor mockClusterInfoProvide = mock(ClusterInfoAccessor.class);

    when(mockClusterInfoProvide.getTaskStates(MinionConstants.MergeRollupTask.TASK_TYPE)).thenReturn(new HashMap<>());
    OfflineSegmentZKMetadata metadata1 =
        getOfflineSegmentZKMetadata("testTable__0", 5000, 50_000, TimeUnit.MILLISECONDS, null);
    when(mockClusterInfoProvide.getOfflineSegmentsMetadata(OFFLINE_TABLE_NAME)).thenReturn(
        Lists.newArrayList(metadata1));

    MergeRollupTaskGenerator generator = new MergeRollupTaskGenerator();
    generator.init(mockClusterInfoProvide);

    // Skip task generation, if realtime table
    TableConfig offlineTableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).build();
    List<PinotTaskConfig> pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertTrue(pinotTaskConfigs.isEmpty());

    // No tableTaskConfig, error
    offlineTableConfig = getOfflineTableConfig(new HashMap<>());
    offlineTableConfig.setTaskConfig(null);
    try {
      generator.generateTasks(Lists.newArrayList(offlineTableConfig));
      Assert.fail("Should have failed for null tableTaskConfig");
    } catch (IllegalStateException e) {
      // expected
    }

    // No taskConfig for task, error
    offlineTableConfig = getOfflineTableConfig(new HashMap<>());
    try {
      generator.generateTasks(Lists.newArrayList(offlineTableConfig));
      Assert.fail("Should have failed for null taskConfig");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  private void checkPinotTaskConfig(Map<String, String> pinotTaskConfig, String tableName, String segments,
      String startTimeMs, String endTimeMs, String mergeType, String bufferTimePeriod, String bucketTimePeriod,
      String maxNumRecordsPerSegments) {
    assertEquals(pinotTaskConfig.get(MinionConstants.TABLE_NAME_KEY), tableName);
    assertEquals(pinotTaskConfig.get(MinionConstants.SEGMENT_NAME_KEY), segments);
    assertEquals(pinotTaskConfig.get(MinionConstants.MergeRollupTask.WINDOW_START_MS_KEY), startTimeMs);
    assertEquals(pinotTaskConfig.get(MinionConstants.MergeRollupTask.WINDOW_END_MS_KEY), endTimeMs);
    assertEquals(pinotTaskConfig.get(MinionConstants.MergeRollupTask.MERGE_TYPE_KEY), mergeType);
    assertEquals(pinotTaskConfig.get(MinionConstants.MergeRollupTask.BUFFER_TIME_PERIOD), bufferTimePeriod);
    assertEquals(pinotTaskConfig.get(MinionConstants.MergeRollupTask.BUCKET_TIME_PERIOD), bucketTimePeriod);
    assertEquals(pinotTaskConfig.get(MinionConstants.MergeRollupTask.MAX_NUM_RECORDS_PER_SEGMENT),
        maxNumRecordsPerSegments);
    assertTrue(pinotTaskConfig.get(MinionConstants.MergeRollupTask.SEGMENT_NAME_PREFIX_KEY)
        .startsWith(MinionConstants.MergeRollupTask.SEGMENT_NAME_PREFIX));
  }

  /**
   * Test update watermark
   */
  @Test
  public void testUpdateWatermark() {
    Map<String, Map<String, String>> taskConfigsMap = new HashMap<>();
    Map<String, String> tableTaskConfigs = new HashMap<>();
    tableTaskConfigs.put("daily.mergeType", "concat");
    tableTaskConfigs.put("daily.bufferTimePeriod", "2d");
    tableTaskConfigs.put("daily.bucketTimePeriod", "1d");
    tableTaskConfigs.put("daily.maxNumRecordsPerSegment", "1000000");
    tableTaskConfigs.put("daily.maxNumRecordsPerTask", "5000000");
    taskConfigsMap.put(MinionConstants.MergeRollupTask.TASK_TYPE, tableTaskConfigs);
    TableConfig offlineTableConfig = getOfflineTableConfig(taskConfigsMap);
    ClusterInfoAccessor mockClusterInfoProvide = mock(ClusterInfoAccessor.class);

    String segmentName1 = "testTable__1";
    String segmentName2 = "testTable__2";
    OfflineSegmentZKMetadata metadata1 =
        getOfflineSegmentZKMetadata(segmentName1, 86_400_000L, 90_000_000L, TimeUnit.MILLISECONDS, null);
    OfflineSegmentZKMetadata metadata2 =
        getOfflineSegmentZKMetadata(segmentName2, 90_000_000L, 180_000_000L, TimeUnit.MILLISECONDS, null);
    when(mockClusterInfoProvide.getOfflineSegmentsMetadata(OFFLINE_TABLE_NAME)).thenReturn(
        Lists.newArrayList(metadata1, metadata2));
    when(mockClusterInfoProvide.getMinionMergeRollupTaskMetadata(OFFLINE_TABLE_NAME)).thenReturn(
        new MergeRollupTaskMetadata(OFFLINE_TABLE_NAME, ImmutableMap.of(DAILY, 0L)));

    MergeRollupTaskGenerator generator = new MergeRollupTaskGenerator();
    generator.init(mockClusterInfoProvide);
    List<PinotTaskConfig> pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertEquals(pinotTaskConfigs.size(), 0);
    verify(mockClusterInfoProvide).setMergeRollupTaskMetadata(
        argThat(metadata -> metadata.getWatermarkMap().get(DAILY) == 86400000L));
  }

  /**
   * Test buffer time
   */
  @Test
  public void testBufferTime() {
    Map<String, Map<String, String>> taskConfigsMap = new HashMap<>();
    Map<String, String> tableTaskConfigs = new HashMap<>();
    tableTaskConfigs.put("daily.mergeType", "concat");
    tableTaskConfigs.put("daily.bufferTimePeriod", "1d");
    tableTaskConfigs.put("daily.bucketTimePeriod", "1d");
    tableTaskConfigs.put("daily.maxNumRecordsPerSegment", "1000000");
    tableTaskConfigs.put("daily.maxNumRecordsPerTask", "5000000");
    taskConfigsMap.put(MinionConstants.MergeRollupTask.TASK_TYPE, tableTaskConfigs);
    TableConfig offlineTableConfig = getOfflineTableConfig(taskConfigsMap);
    ClusterInfoAccessor mockClusterInfoProvide = mock(ClusterInfoAccessor.class);

    String segmentName1 = "testTable__1";
    long currentTime = System.currentTimeMillis();
    OfflineSegmentZKMetadata metadata1 =
        getOfflineSegmentZKMetadata(segmentName1, currentTime - 500_000L, currentTime, TimeUnit.MILLISECONDS, null);
    when(mockClusterInfoProvide.getOfflineSegmentsMetadata(OFFLINE_TABLE_NAME)).thenReturn(
        Lists.newArrayList(metadata1));

    MergeRollupTaskGenerator generator = new MergeRollupTaskGenerator();
    generator.init(mockClusterInfoProvide);
    List<PinotTaskConfig> pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertEquals(pinotTaskConfigs.size(), 0);
  }

  /**
   * Test max number records per task
   */
  @Test
  public void testMaxNumRecordsPerTask() {
    Map<String, Map<String, String>> taskConfigsMap = new HashMap<>();
    Map<String, String> tableTaskConfigs = new HashMap<>();
    tableTaskConfigs.put("daily.mergeType", "concat");
    tableTaskConfigs.put("daily.bufferTimePeriod", "2d");
    tableTaskConfigs.put("daily.bucketTimePeriod", "1d");
    tableTaskConfigs.put("daily.maxNumRecordsPerSegment", "1000000");
    tableTaskConfigs.put("daily.maxNumRecordsPerTask", "5000000");
    taskConfigsMap.put(MinionConstants.MergeRollupTask.TASK_TYPE, tableTaskConfigs);
    TableConfig offlineTableConfig = getOfflineTableConfig(taskConfigsMap);
    ClusterInfoAccessor mockClusterInfoProvide = mock(ClusterInfoAccessor.class);

    String segmentName1 = "testTable__1";
    String segmentName2 = "testTable__2";
    String segmentName3 = "testTable__3";
    OfflineSegmentZKMetadata metadata1 =
        getOfflineSegmentZKMetadata(segmentName1, 86_400_000L, 90_000_000L, TimeUnit.MILLISECONDS, null);
    metadata1.setTotalDocs(2000000L);
    OfflineSegmentZKMetadata metadata2 =
        getOfflineSegmentZKMetadata(segmentName2, 86_400_000L, 100_000_000L, TimeUnit.MILLISECONDS, null);
    metadata2.setTotalDocs(3000000L);
    OfflineSegmentZKMetadata metadata3 =
        getOfflineSegmentZKMetadata(segmentName3, 86_400_000L, 110_000_000L, TimeUnit.MILLISECONDS, null);
    metadata3.setTotalDocs(4000000L);
    when(mockClusterInfoProvide.getOfflineSegmentsMetadata(OFFLINE_TABLE_NAME)).thenReturn(
        Lists.newArrayList(metadata1, metadata2, metadata3));

    MergeRollupTaskGenerator generator = new MergeRollupTaskGenerator();
    generator.init(mockClusterInfoProvide);
    List<PinotTaskConfig> pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertEquals(pinotTaskConfigs.size(), 1);
    checkPinotTaskConfig(pinotTaskConfigs.get(0).getConfigs(), OFFLINE_TABLE_NAME, segmentName1 + "," + segmentName2,
        "86400000", "172800000", "CONCAT", "2d", "1d", "1000000");
  }

  /**
   * Tests for segment selection
   */
  @Test
  public void testSegmentSelectionSingleLevel() {
    // Setup
    Map<String, Map<String, String>> taskConfigsMap = new HashMap<>();
    Map<String, String> tableTaskConfigs = new HashMap<>();
    tableTaskConfigs.put("daily.mergeType", "concat");
    tableTaskConfigs.put("daily.bufferTimePeriod", "2d");
    tableTaskConfigs.put("daily.bucketTimePeriod", "1d");
    tableTaskConfigs.put("daily.maxNumRecordsPerSegment", "1000000");
    tableTaskConfigs.put("daily.maxNumRecordsPerTask", "5000000");
    taskConfigsMap.put(MinionConstants.MergeRollupTask.TASK_TYPE, tableTaskConfigs);
    TableConfig offlineTableConfig = getOfflineTableConfig(taskConfigsMap);
    ClusterInfoAccessor mockClusterInfoProvide = mock(ClusterInfoAccessor.class);

    // No segments
    MergeRollupTaskGenerator generator = new MergeRollupTaskGenerator();
    generator.init(mockClusterInfoProvide);
    List<PinotTaskConfig> pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertTrue(pinotTaskConfigs.isEmpty());

    // Segments without lineage, cold start without watermark
    String segmentName1 = "testTable__1";
    String segmentName2 = "testTable__2";
    String segmentName3 = "testTable__3";
    String segmentName4 = "testTable__4";
    OfflineSegmentZKMetadata metadata1 =
        getOfflineSegmentZKMetadata(segmentName1, 86_400_000L, 90_000_000L, TimeUnit.MILLISECONDS, null);
    OfflineSegmentZKMetadata metadata2 =
        getOfflineSegmentZKMetadata(segmentName2, 90_000_000L, 180_000_000L, TimeUnit.MILLISECONDS, null);
    OfflineSegmentZKMetadata metadata3 =
        getOfflineSegmentZKMetadata(segmentName3, 86_400_000L, 172_799_999L, TimeUnit.MILLISECONDS, null);
    OfflineSegmentZKMetadata metadata4 =
        getOfflineSegmentZKMetadata(segmentName4, 200_000_000L, 250_000_000L, TimeUnit.MILLISECONDS, null);
    when(mockClusterInfoProvide.getOfflineSegmentsMetadata(OFFLINE_TABLE_NAME)).thenReturn(
        Lists.newArrayList(metadata1, metadata2, metadata3, metadata4));
    generator.init(mockClusterInfoProvide);
    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertEquals(pinotTaskConfigs.size(), 1);
    Map<String, String> pinotTaskConfig = pinotTaskConfigs.get(0).getConfigs();
    checkPinotTaskConfig(pinotTaskConfig, OFFLINE_TABLE_NAME, segmentName1 + "," + segmentName3 + "," + segmentName2,
        "86400000", "172800000", "CONCAT", "2d", "1d", "1000000");

    // With watermark
    when(mockClusterInfoProvide.getMinionMergeRollupTaskMetadata(OFFLINE_TABLE_NAME)).thenReturn(
        new MergeRollupTaskMetadata(OFFLINE_TABLE_NAME, ImmutableMap.of(DAILY, 86400000L)));
    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertEquals(pinotTaskConfigs.size(), 1);
    pinotTaskConfig = pinotTaskConfigs.get(0).getConfigs();
    checkPinotTaskConfig(pinotTaskConfig, OFFLINE_TABLE_NAME, segmentName1 + "," + segmentName3 + "," + segmentName2,
        "86400000", "172800000", "CONCAT", "2d", "1d", "1000000");

    // Segments with lineage entries and custom map config
    when(mockClusterInfoProvide.getMinionMergeRollupTaskMetadata(OFFLINE_TABLE_NAME)).thenReturn(
        new MergeRollupTaskMetadata(OFFLINE_TABLE_NAME, ImmutableMap.of(DAILY, 86400000L)));
    SegmentLineage segmentLineage = new SegmentLineage(OFFLINE_TABLE_NAME);
    segmentLineage.addLineageEntry(SegmentLineageUtils.generateLineageEntryId(),
        new LineageEntry(Arrays.asList(segmentName1, segmentName3), Arrays.asList("merged_segment1"),
            LineageEntryState.COMPLETED, 11111L));
    segmentLineage.addLineageEntry(SegmentLineageUtils.generateLineageEntryId(),
        new LineageEntry(Arrays.asList(segmentName2), Arrays.asList("merged_segment2", "merged_segment3"),
            LineageEntryState.IN_PROGRESS, 11111L));
    when(mockClusterInfoProvide.getSegmentLineage(OFFLINE_TABLE_NAME)).thenReturn(segmentLineage);
    OfflineSegmentZKMetadata metadata_merged =
        getOfflineSegmentZKMetadata("merged_segment1", 86_400_000, 172_799_999, TimeUnit.MILLISECONDS, null);
    metadata_merged.setCustomMap(
        ImmutableMap.of(MinionConstants.MergeRollupTask.TASK_TYPE + MinionConstants.TASK_BUCKET_GRANULARITY_SUFFIX, DAILY));
    when(mockClusterInfoProvide.getOfflineSegmentsMetadata(OFFLINE_TABLE_NAME)).thenReturn(
        Lists.newArrayList(metadata1, metadata2, metadata3, metadata4, metadata_merged));

    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertEquals(pinotTaskConfigs.size(), 1);
    pinotTaskConfig = pinotTaskConfigs.get(0).getConfigs();
    checkPinotTaskConfig(pinotTaskConfig, OFFLINE_TABLE_NAME, segmentName2, "86400000", "172800000", "CONCAT", "2d",
        "1d", "1000000");

    // Segments with incomplete tasks
    Map<String, TaskState> taskStatesMap = new HashMap<>();
    String taskName = "Task_MergeRollupTask_" + System.currentTimeMillis();

    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(MinionConstants.TABLE_NAME_KEY, OFFLINE_TABLE_NAME);
    taskConfigs.put(MinionConstants.MergeRollupTask.GRANULARITY_KEY, DAILY);
    taskConfigs.put(MinionConstants.SEGMENT_NAME_KEY, segmentName2);
    when(mockClusterInfoProvide.getTaskStates(MinionConstants.MergeRollupTask.TASK_TYPE)).thenReturn(taskStatesMap);
    when(mockClusterInfoProvide.getTaskConfigs(taskName)).thenReturn(
        Lists.newArrayList(new PinotTaskConfig(MinionConstants.MergeRollupTask.TASK_TYPE, taskConfigs)));
    when(mockClusterInfoProvide.getOfflineSegmentsMetadata(OFFLINE_TABLE_NAME)).thenReturn(
        Lists.newArrayList(metadata1, metadata2, metadata3, metadata4, metadata_merged));

    taskStatesMap.put(taskName, TaskState.IN_PROGRESS);
    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertTrue(pinotTaskConfigs.isEmpty());

    taskStatesMap.put(taskName, TaskState.COMPLETED);
    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertEquals(pinotTaskConfigs.size(), 1);
    checkPinotTaskConfig(pinotTaskConfig, OFFLINE_TABLE_NAME, segmentName2, "86400000", "172800000", "CONCAT", "2d",
        "1d", "1000000");

    String oldTaskName = "Task_MergeRollupTask_" + (System.currentTimeMillis() - TimeUnit.DAYS.toMillis(3));
    taskStatesMap.remove(taskName);
    taskStatesMap.put(oldTaskName, TaskState.IN_PROGRESS);
    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertEquals(pinotTaskConfigs.size(), 1);
    checkPinotTaskConfig(pinotTaskConfig, OFFLINE_TABLE_NAME, segmentName2, "86400000", "172800000", "CONCAT", "2d",
        "1d", "1000000");
  }

  /**
   * Tests for multilevel selection
   */
  @Test
  public void testSegmentSelectionMultiLevels() {
    Map<String, Map<String, String>> taskConfigsMap = new HashMap<>();
    Map<String, String> tableTaskConfigs = new HashMap<>();
    tableTaskConfigs.put("daily.mergeType", "concat");
    tableTaskConfigs.put("daily.bufferTimePeriod", "2d");
    tableTaskConfigs.put("daily.bucketTimePeriod", "1d");
    tableTaskConfigs.put("daily.maxNumRecordsPerSegment", "1000000");
    tableTaskConfigs.put("daily.maxNumRecordsPerTask", "5000000");

    tableTaskConfigs.put("monthly.mergeType", "concat");
    tableTaskConfigs.put("monthly.bufferTimePeriod", "30d");
    tableTaskConfigs.put("monthly.bucketTimePeriod", "30d");
    tableTaskConfigs.put("monthly.maxNumRecordsPerSegment", "2000000");
    tableTaskConfigs.put("monthly.maxNumRecordsPerTask", "5000000");

    taskConfigsMap.put(MinionConstants.MergeRollupTask.TASK_TYPE, tableTaskConfigs);
    TableConfig offlineTableConfig = getOfflineTableConfig(taskConfigsMap);
    ClusterInfoAccessor mockClusterInfoProvide = mock(ClusterInfoAccessor.class);

    String segmentName1 = "testTable__1";
    String segmentName2 = "testTable__2";
    String segmentName3 = "testTable__3";
    OfflineSegmentZKMetadata metadata1 =
        getOfflineSegmentZKMetadata(segmentName1, 86_400_000L, 90_000_000L, TimeUnit.MILLISECONDS, null);
    OfflineSegmentZKMetadata metadata2 =
        getOfflineSegmentZKMetadata(segmentName2, 86_400_000L, 100_000_000L, TimeUnit.MILLISECONDS, null);
    OfflineSegmentZKMetadata metadata3 =
        getOfflineSegmentZKMetadata(segmentName3, 86_400_000L, 110_000_000L, TimeUnit.MILLISECONDS, null);
    when(mockClusterInfoProvide.getOfflineSegmentsMetadata(OFFLINE_TABLE_NAME)).thenReturn(
        Lists.newArrayList(metadata1, metadata2, metadata3));

    // Cold start only schedule daily merge tasks
    MergeRollupTaskGenerator generator = new MergeRollupTaskGenerator();
    generator.init(mockClusterInfoProvide);
    List<PinotTaskConfig> pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertEquals(pinotTaskConfigs.size(), 1);
    checkPinotTaskConfig(pinotTaskConfigs.get(0).getConfigs(), OFFLINE_TABLE_NAME,
        segmentName1 + "," + segmentName2 + "," + segmentName3, "86400000", "172800000", "CONCAT", "2d", "1d",
        "1000000");

    // Monthly task is not scheduled if endTimeMs > watermark of daily granularity
    String segmentNameMergedDaily = "merged_testTable__123";
    String segmentName4 = "segmentName4";
    OfflineSegmentZKMetadata metadata_merged_daily =
        getOfflineSegmentZKMetadata(segmentNameMergedDaily, 86_400_000L, 110_000_000L, TimeUnit.MILLISECONDS, null);
    metadata_merged_daily.setCustomMap(
        ImmutableMap.of(MinionConstants.MergeRollupTask.TASK_TYPE + MinionConstants.TASK_BUCKET_GRANULARITY_SUFFIX, DAILY));
    OfflineSegmentZKMetadata metadata4 =
        getOfflineSegmentZKMetadata(segmentName4, 2_592_000_000L, 2_592_010_000L, TimeUnit.MILLISECONDS, null);
    HashMap<String, Long> waterMarkMap = new HashMap<>();
    waterMarkMap.put(DAILY, 2_419_200_000L);
    when(mockClusterInfoProvide.getMinionMergeRollupTaskMetadata(OFFLINE_TABLE_NAME)).thenReturn(
        new MergeRollupTaskMetadata(OFFLINE_TABLE_NAME, waterMarkMap));
    when(mockClusterInfoProvide.getOfflineSegmentsMetadata(OFFLINE_TABLE_NAME)).thenReturn(
        Lists.newArrayList(metadata_merged_daily, metadata4));
    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertEquals(pinotTaskConfigs.size(), 0);

    // Start scheduling monthly task once endTimeMs <= watermark of daily granularity
    waterMarkMap.put(DAILY, 2_505_600_000L);
    when(mockClusterInfoProvide.getMinionMergeRollupTaskMetadata(OFFLINE_TABLE_NAME)).thenReturn(
        new MergeRollupTaskMetadata(OFFLINE_TABLE_NAME, waterMarkMap));
    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertEquals(pinotTaskConfigs.size(), 1);
    checkPinotTaskConfig(pinotTaskConfigs.get(0).getConfigs(), OFFLINE_TABLE_NAME, segmentNameMergedDaily, "0",
        "2592000000", "CONCAT", "30d", "30d", "2000000");

    // Schedule multiple tasks simultaneously, one for each granularity
    String segmentName5 = "segmentName5";
    waterMarkMap.put(DAILY, 2_592_000_000L);
    when(mockClusterInfoProvide.getMinionMergeRollupTaskMetadata(OFFLINE_TABLE_NAME)).thenReturn(
        new MergeRollupTaskMetadata(OFFLINE_TABLE_NAME, waterMarkMap));
    pinotTaskConfigs = generator.generateTasks(Lists.newArrayList(offlineTableConfig));
    assertEquals(pinotTaskConfigs.size(), 2);
    checkPinotTaskConfig(pinotTaskConfigs.get(0).getConfigs(), OFFLINE_TABLE_NAME, segmentName4, "2592000000",
        "2678400000", "CONCAT", "2d", "1d", "1000000");
    checkPinotTaskConfig(pinotTaskConfigs.get(1).getConfigs(), OFFLINE_TABLE_NAME, segmentNameMergedDaily, "0",
        "2592000000", "CONCAT", "30d", "30d", "2000000");
  }

  private OfflineSegmentZKMetadata getOfflineSegmentZKMetadata(String segmentName, long startTime, long endTime,
      TimeUnit timeUnit, String downloadURL) {
    OfflineSegmentZKMetadata offlineSegmentZKMetadata = new OfflineSegmentZKMetadata();
    offlineSegmentZKMetadata.setSegmentName(segmentName);
    offlineSegmentZKMetadata.setStartTime(startTime);
    offlineSegmentZKMetadata.setEndTime(endTime);
    offlineSegmentZKMetadata.setTimeUnit(timeUnit);
    offlineSegmentZKMetadata.setDownloadUrl(downloadURL);
    return offlineSegmentZKMetadata;
  }
}
