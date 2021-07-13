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

public class MergeProperties {
  private final String _mergeType;
  private final String _bufferTimePeriod;
  private final String _bucketTimePeriod;
  private final String _roundBucketTimePeriod;
  private final long _maxNumRecordsPerSegment;
  private final long _maxNumRecordsPerTask;

  public MergeProperties(String mergeType, String bufferTimePeriod, String bucketTimePeriod,
      String roundBucketTimePeriod, long maxNumRecordsPerSegment, long maxNumRecordsPerTask) {
    _mergeType = mergeType;
    _bufferTimePeriod = bufferTimePeriod;
    _bucketTimePeriod = bucketTimePeriod;
    _roundBucketTimePeriod = roundBucketTimePeriod;
    _maxNumRecordsPerSegment = maxNumRecordsPerSegment;
    _maxNumRecordsPerTask = maxNumRecordsPerTask;
  }

  public String getMergeType() {
    return _mergeType;
  }

  public String getBufferTimePeriod() {
    return _bufferTimePeriod;
  }

  public String getBucketTimePeriod() {
    return _bucketTimePeriod;
  }

  public String getRoundBucketTimePeriod() {
    return _roundBucketTimePeriod;
  }

  public long getMaxNumRecordsPerSegment() {
    return _maxNumRecordsPerSegment;
  }

  public long getMaxNumRecordsPerTask() {
    return _maxNumRecordsPerTask;
  }
}
