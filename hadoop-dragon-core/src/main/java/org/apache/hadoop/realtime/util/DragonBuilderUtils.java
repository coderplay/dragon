/**
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.realtime.util;

import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.realtime.job.TaskAttempt;
import org.apache.hadoop.realtime.records.AMInfo;
import org.apache.hadoop.realtime.records.ChildExecutionContext;
import org.apache.hadoop.realtime.records.Counter;
import org.apache.hadoop.realtime.records.CounterGroup;
import org.apache.hadoop.realtime.records.Counters;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.records.JobReport;
import org.apache.hadoop.realtime.records.JobState;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;

public class DragonBuilderUtils {

  public static JobReport newJobReport(JobId jobId, String jobName,
      String userName, JobState state, long submitTime, long startTime,
      long finishTime, String jobFile, List<AMInfo> amInfos, boolean isUber) {
    JobReport report = Records.newRecord(JobReport.class);
    report.setJobId(jobId);
    report.setJobName(jobName);
    report.setUser(userName);
    report.setJobState(state);
    report.setSubmitTime(submitTime);
    report.setStartTime(startTime);
    report.setFinishTime(finishTime);
    report.setJobFile(jobFile);
    report.setAMInfos(amInfos);
    report.setIsUber(isUber);
    return report;
  }

  public static AMInfo newAMInfo(ApplicationAttemptId appAttemptId,
      long startTime, ContainerId containerId, String nmHost, int nmPort,
      int nmHttpPort) {
    AMInfo amInfo = Records.newRecord(AMInfo.class);
    amInfo.setAppAttemptId(appAttemptId);
    amInfo.setStartTime(startTime);
    amInfo.setContainerId(containerId);
    amInfo.setNodeManagerHost(nmHost);
    amInfo.setNodeManagerPort(nmPort);
    amInfo.setNodeManagerHttpPort(nmHttpPort);
    return amInfo;
  }
  
  public static ContainerId newContainerId(String str)
      throws IllegalArgumentException {
    if (str == null)
      return null;
    try {
      String[] parts = str.split("_");
      if (parts.length == 5 && parts[0].equals("container")) {
        long timestamp = Long.parseLong(parts[1]);
        int appId = Integer.parseInt(parts[2]);
        int appAttemptId = Integer.parseInt(parts[3]);
        int id = Integer.parseInt(parts[4]);
        return BuilderUtils.newContainerId(appId, appAttemptId, timestamp, id);
      }
    } catch (Exception ex) {
      // fall through
    }
    throw new IllegalArgumentException("ContainerId string : " + str
        + " is not properly formed");
  }

  public static ChildExecutionContext newTaskAttemptExecutionContext(
      TaskAttempt attempt, String user) {
    ChildExecutionContext context =
        Records.newRecord(ChildExecutionContext.class);
    context.setTaskAttemptId(attempt.getID());
    context.setPartition(attempt.getPartition());
    context.setUser(user);
    context.setTaskType(attempt.getTaskType());
    return context;
  }
  
  public static Counter newCounter(String name){
    Counter counter = Records.newRecord(Counter.class);
    counter.setName(name);
    counter.setValue(0);
    return counter;
  }
  
  public static Counters newCounters(){
    Counters counters = Records.newRecord(Counters.class);
    counters.addAllCounterGroups(new ConcurrentSkipListMap<String, CounterGroup>());
    return counters;
  }
}