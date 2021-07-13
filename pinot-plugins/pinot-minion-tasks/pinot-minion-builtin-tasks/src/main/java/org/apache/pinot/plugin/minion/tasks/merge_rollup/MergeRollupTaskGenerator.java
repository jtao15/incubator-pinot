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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.lineage.LineageEntry;
import org.apache.pinot.common.lineage.LineageEntryState;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.minion.MergeRollupTaskMetadata;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import org.apache.pinot.controller.helix.core.minion.generator.TaskGeneratorUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.MergeRollupTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.annotations.minion.TaskGenerator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link PinotTaskGenerator} implementation for generating tasks of type {@link MergeRollupTask}
 *
 * TODO:
 *  1. Add the support for roll-up
 *  2. Add the support for customer partitioned table
 *  3. Add the support for realtime table
 *
 * Steps:
 *
 *  - Select segments:
 *    - Fetch all segments, select segments based on segment lineage (removing segmentsFrom for COMPLETED lineage entry and
 *      segmentsTo for IN_PROGRESS lineage entry)
 *    - Filter out segments already scheduled by checking zk minion task configs
 *
 *  For each granularity (from lowest to highest, e.g. Hourly -> Daily -> Monthly -> Yearly):
 *    - Calculate merge/rollup window:
 *      - Read watermarkMs from the {@link MergeRollupTaskMetadata} ZNode
 *        found at MINION_TASK_METADATA/MergeRollupTaskMetadata/tableNameWithType
 *        In case of cold-start, no ZNode will exist.
 *        A new ZNode will be created, with watermarkMs as the smallest time found in all segments truncated to the
 *        closest bucket start time.
 *      - The execution window for the task is calculated as,
 *        windowStartMs = watermarkMs, windowEndMs = windowStartMs + bucketTimeMs
 *      - Skip scheduling if the window is invalid:
 *        - If the execution window is not older than bufferTimeMs, no task will be generated
 *        - The windowEndMs for higher granularity should be less or equal than the waterMarkMs for lower granularity
 *
 *    - Select target segments:
 *      - Filter out segments already merged by checking segment zk metadata {mergeRollupTask.bucketGranularity: granularity}
 *      - Pick segments overlapping with window [windowStartMs, windowEndMs) up to maxNumRecordsPerTask in sorted order
 *
 *    - Bump up waterMarkMs if needed:
 *      - If there's no segment in the execution window, then everything is merged for the window, bump up the watermark as
 *        watermarkMs += bucketTimeMs and skip the scheduling.
 *
 *    - Create task config
 */
@TaskGenerator
public class MergeRollupTaskGenerator implements PinotTaskGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MergeRollupTaskGenerator.class);

  private ClusterInfoAccessor _clusterInfoAccessor;

  @Override
  public void init(ClusterInfoAccessor clusterInfoAccessor) {
    _clusterInfoAccessor = clusterInfoAccessor;
  }

  @Override
  public String getTaskType() {
    return MergeRollupTask.TASK_TYPE;
  }

  @Override
  public List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs) {
    String taskType = MergeRollupTask.TASK_TYPE;
    List<PinotTaskConfig> pinotTaskConfigs = new ArrayList<>();

    for (TableConfig tableConfig : tableConfigs) {
      String offlineTableName = tableConfig.getTableName();

      if (tableConfig.getTableType() != TableType.OFFLINE) {
        LOGGER.warn("Skip generating task: {} for non-OFFLINE table: {}", taskType, offlineTableName);
        continue;
      }

      if (tableConfig.getIndexingConfig().getSegmentPartitionConfig() != null) {
        LOGGER.warn("Skip generating task: {} for table: {} with customer partition", taskType, offlineTableName);
        continue;
      }

      TableTaskConfig tableTaskConfig = tableConfig.getTaskConfig();
      Preconditions.checkState(tableTaskConfig != null);
      Map<String, String> taskConfigs = tableTaskConfig.getConfigsForTaskType(taskType);
      Preconditions.checkState(taskConfigs != null, "Task config shouldn't be null for table: {}", offlineTableName);

      // Get all segment metadata
      Set<OfflineSegmentZKMetadata> segmentsForOfflineTable =
          _clusterInfoAccessor.getOfflineSegmentsMetadata(offlineTableName).stream().collect(Collectors.toSet());

      // Select current segment snapshot based on lineage
      Set<String> segmentsNotToMerge = new HashSet<>();
      SegmentLineage segmentLineageForTable = _clusterInfoAccessor.getSegmentLineage(offlineTableName);
      if (segmentLineageForTable != null) {
        for (String segmentLineageEntryId : segmentLineageForTable.getLineageEntryIds()) {
          LineageEntry lineageEntry = segmentLineageForTable.getLineageEntry(segmentLineageEntryId);
          // Segments shows up on "segmentFrom" field in the lineage entry with "COMPLETED" state cannot be merged.
          if (lineageEntry.getState() == LineageEntryState.COMPLETED) {
            segmentsNotToMerge.addAll(lineageEntry.getSegmentsFrom());
          }

          // Segments shows up on "segmentsTo" field in the lineage entry with "IN_PROGRESS" state cannot be merged.
          if (lineageEntry.getState() == LineageEntryState.IN_PROGRESS) {
            segmentsNotToMerge.addAll(lineageEntry.getSegmentsTo());
          }
        }
      }

      LOGGER.info("Start generating task configs for table: {} for task: {}", offlineTableName, taskType);

      Map<String, MergeProperties> mergeProperties = MergeRollupTaskUtils.getAllMergeProperties(taskConfigs);
      Map<String, PinotTaskConfig> inCompleteGranularities = new HashMap<>();

      // Filter out segments that are already scheduled
      for (Map.Entry<String, TaskState> entry : TaskGeneratorUtils.getIncompleteTasks(taskType, offlineTableName,
          _clusterInfoAccessor).entrySet()) {
        for (PinotTaskConfig taskConfig : _clusterInfoAccessor.getTaskConfigs(entry.getKey())) {
          inCompleteGranularities.put(taskConfig.getConfigs().get(MergeRollupTask.GRANULARITY_KEY), taskConfig);
          Arrays.stream(taskConfig.getConfigs()
              .get(MinionConstants.SEGMENT_NAME_KEY)
              .split(MinionConstants.SEGMENT_NAME_SEPARATOR)).forEach(s -> segmentsNotToMerge.add(s));
        }
      }

      segmentsForOfflineTable = segmentsForOfflineTable.stream()
          .filter(s -> !segmentsNotToMerge.contains(s.getSegmentName()))
          .collect(Collectors.toSet());
      if (segmentsForOfflineTable.isEmpty()) {
        LOGGER.warn("Skip generating task: {} for table: {}, no segment is found to merge.", taskType,
            offlineTableName);
        continue;
      }

      // From lowest to highest granularity
      String lowerGranularity = null;
      long lowerGranularityWatermarkMs = -1;
      for (Map.Entry<String, MergeProperties> entry : mergeProperties.entrySet()
          .stream()
          .sorted((e1, e2) -> Long.compare(TimeUtils.convertPeriodToMillis(e1.getValue().getBucketTimePeriod()),
              TimeUtils.convertPeriodToMillis(e2.getValue().getBucketTimePeriod())))
          .collect(Collectors.toList())) {
        String granularity = entry.getKey();
        // Only schedule 1 task per granularity
        if (inCompleteGranularities.containsKey(granularity)) {
          LOGGER.info("Found incomplete tasks: {} for same granularity: {} and table: {}. Skipping task generation.",
              inCompleteGranularities.get(granularity), granularity, offlineTableName);
          continue;
        }

        MergeProperties mergeProperty = entry.getValue();
        Set<OfflineSegmentZKMetadata> candidateSegments = new HashSet<>(segmentsForOfflineTable);

        // Get the bucket size and buffer
        long bucketMs = TimeUtils.convertPeriodToMillis(mergeProperty.getBucketTimePeriod());
        long bufferMs = TimeUtils.convertPeriodToMillis(mergeProperty.getBufferTimePeriod());

        // Get watermark from MergeRollupTaskMetadata ZNode. WindowStart = watermark. WindowEnd = windowStart + bucket.
        long windowStartMs = getWatermarkMs(offlineTableName, candidateSegments, bucketMs, granularity);
        long windowEndMs = windowStartMs + bucketMs;

        // Check that execution window endTimeMs <= now - bufferTime
        if (windowEndMs > System.currentTimeMillis() - bufferMs) {
          LOGGER.info(
              "Window with start: {} and end: {} is not older than buffer time: {} configured as {} ago. Skipping task generation: {}",
              windowStartMs, windowEndMs, bufferMs, bufferMs, taskType);
          lowerGranularity = granularity;
          lowerGranularityWatermarkMs = windowStartMs;
          continue;
        }

        // Check that execution window endTimeMs <= waterMark of the lower granularity
        if (lowerGranularity != null && windowEndMs > lowerGranularityWatermarkMs) {
          LOGGER.info(
              "Window with start: {} and end: {} of granularity: {} is not older than the watermark: {} of granularity: (). Skipping task generation: {}",
              windowStartMs, windowEndMs, granularity, lowerGranularityWatermarkMs, lowerGranularity, taskType);
          lowerGranularity = granularity;
          lowerGranularityWatermarkMs = windowStartMs;
          continue;
        }

        // Filter out segments that are already merged for the granularity
        candidateSegments = candidateSegments.stream()
            .filter(s -> s.getCustomMap() != null ? !granularity.equalsIgnoreCase(
                s.getCustomMap().get(taskType + MinionConstants.TASK_BUCKET_GRANULARITY_SUFFIX)) : true)
            .collect(Collectors.toSet());

        // Find segments with data overlapping execution window: windowStart (inclusive) to windowEnd (exclusive),
        // sort segments based on start time, if start time is the same, sort on end time. Pick segments up to
        // maxNumRecordsPerTask (soft boundary).
        List<OfflineSegmentZKMetadata> targetSegments = new ArrayList<>();
        for (OfflineSegmentZKMetadata offlineSegmentZKMetadata : candidateSegments) {
          // check overlap with the window
          if (windowStartMs <= offlineSegmentZKMetadata.getEndTimeMs()
              && offlineSegmentZKMetadata.getStartTimeMs() < windowEndMs) {
            targetSegments.add(offlineSegmentZKMetadata);
          }
        }

        if (targetSegments.isEmpty()) {
          LOGGER.info("No segments overlapping with window [{}, {}), Skipping task generation: {}", windowStartMs,
              windowEndMs, taskType);
          // Bump up watermark
          updateWatermarkMs(offlineTableName, granularity, windowStartMs + bucketMs);
          lowerGranularity = granularity;
          lowerGranularityWatermarkMs = windowStartMs + bucketMs;
          continue;
        }
        lowerGranularity = granularity;
        lowerGranularityWatermarkMs = windowStartMs;

        Collections.sort(targetSegments, (a, b) -> {
          long aStartTime = a.getStartTimeMs();
          long bStartTime = b.getStartTimeMs();
          return aStartTime != bStartTime ? Long.compare(aStartTime, bStartTime)
              : Long.compare(a.getEndTimeMs(), b.getEndTimeMs());
        });

        long numRecordsPerTask = 0L;
        List<String> segmentNames = new ArrayList<>();
        List<String> downloadURLs = new ArrayList<>();
        for (OfflineSegmentZKMetadata targetSegment : targetSegments) {
          segmentNames.add(targetSegment.getSegmentName());
          downloadURLs.add(targetSegment.getDownloadUrl());
          numRecordsPerTask += targetSegment.getTotalDocs();
          if (numRecordsPerTask >= mergeProperty.getMaxNumRecordsPerTask()) {
            break;
          }
        }

        // Create task configs
        Map<String, String> configs = new HashMap<>();
        configs.put(MinionConstants.TABLE_NAME_KEY, offlineTableName);
        configs.put(MinionConstants.SEGMENT_NAME_KEY,
            StringUtils.join(segmentNames, MinionConstants.SEGMENT_NAME_SEPARATOR));
        configs.put(MinionConstants.DOWNLOAD_URL_KEY, StringUtils.join(downloadURLs, MinionConstants.URL_SEPARATOR));
        configs.put(MinionConstants.UPLOAD_URL_KEY, _clusterInfoAccessor.getVipUrl() + "/segments");

        configs.put(MergeRollupTask.WINDOW_START_MS_KEY, String.valueOf(windowStartMs));
        configs.put(MergeRollupTask.WINDOW_END_MS_KEY, String.valueOf(windowEndMs));

        for (Map.Entry<String, String> taskConfig : taskConfigs.entrySet()) {
          if (taskConfig.getKey().endsWith(MinionConstants.MergeRollupTask.AGGREGATION_TYPE_KEY_SUFFIX)) {
            configs.put(taskConfig.getKey(), taskConfig.getValue());
          }
        }

        configs.put(MergeRollupTask.MERGE_TYPE_KEY, mergeProperty.getMergeType());
        configs.put(MergeRollupTask.BUFFER_TIME_PERIOD, String.valueOf(mergeProperty.getBufferTimePeriod()));
        configs.put(MergeRollupTask.BUCKET_TIME_PERIOD, String.valueOf(mergeProperty.getBucketTimePeriod()));
        configs.put(MergeRollupTask.ROUND_BUCKET_TIME_PERIOD, String.valueOf(mergeProperty.getRoundBucketTimePeriod()));
        configs.put(MergeRollupTask.MAX_NUM_RECORDS_PER_SEGMENT,
            String.valueOf(mergeProperty.getMaxNumRecordsPerSegment()));

        configs.put(MergeRollupTask.SEGMENT_NAME_PREFIX_KEY,
            MergeRollupTask.SEGMENT_NAME_PREFIX + "_" + System.currentTimeMillis());

        pinotTaskConfigs.add(new PinotTaskConfig(taskType, configs));
        LOGGER.info("Finished generating task configs for table: {} for task: {}", offlineTableName, taskType);
      }
    }
    return pinotTaskConfigs;
  }

  /**
   * Get the watermark from the MergeRollupMetadata ZNode.
   * If the znode is null, computes the watermark using the start time from segment metadata
   */
  private long getWatermarkMs(String offlineTableName, Set<OfflineSegmentZKMetadata> offlineSegmentsMetadata,
      long bucketMs, String granularity) {
    MergeRollupTaskMetadata mergeRollupTaskMetadata =
        _clusterInfoAccessor.getMinionMergeRollupTaskMetadata(offlineTableName);

    if (mergeRollupTaskMetadata == null || mergeRollupTaskMetadata.getWatermarkMap() == null
        || mergeRollupTaskMetadata.getWatermarkMap().get(granularity) == null) {
      // No ZNode exists. Cold-start.
      long watermarkMs;

      // Find the smallest time from all segments
      long minStartTimeMs = Long.MAX_VALUE;
      for (OfflineSegmentZKMetadata offlineSegmentZKMetadata : offlineSegmentsMetadata) {
        minStartTimeMs = Math.min(minStartTimeMs, offlineSegmentZKMetadata.getStartTimeMs());
      }
      Preconditions.checkState(minStartTimeMs != Long.MAX_VALUE);

      // Round off according to the bucket. This ensures we align the merged segments to proper time boundaries
      // For example, if start time millis is 20200813T12:34:59, we want to create the merged segment for window [20200813, 20200814)
      watermarkMs = (minStartTimeMs / bucketMs) * bucketMs;

      // Create MergeRollupTaskMetadata ZNode using watermark calculated above
      if (mergeRollupTaskMetadata == null || mergeRollupTaskMetadata.getWatermarkMap() == null) {
        mergeRollupTaskMetadata = new MergeRollupTaskMetadata(offlineTableName, new HashMap<>());
      }
      mergeRollupTaskMetadata.getWatermarkMap().put(granularity, watermarkMs);
      _clusterInfoAccessor.setMergeRollupTaskMetadata(mergeRollupTaskMetadata);
    }
    return mergeRollupTaskMetadata.getWatermarkMap().get(granularity);
  }

  /**
   * Update current watermark for granularity
   */
  private void updateWatermarkMs(String offlineTableName, String granularity, long newWatermarkMs) {
    Map<String, Long> waterMarkMap =
        new HashMap<>(_clusterInfoAccessor.getMinionMergeRollupTaskMetadata(offlineTableName).getWatermarkMap());
    waterMarkMap.put(granularity, newWatermarkMs);
    _clusterInfoAccessor.setMergeRollupTaskMetadata(new MergeRollupTaskMetadata(offlineTableName, waterMarkMap));
  }
}
