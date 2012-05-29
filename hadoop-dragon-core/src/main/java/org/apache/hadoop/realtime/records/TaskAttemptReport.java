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

package org.apache.hadoop.realtime.records;

import org.apache.hadoop.yarn.api.records.ContainerId;

public interface TaskAttemptReport {
  public abstract TaskAttemptId getTaskAttemptId();
  public abstract TaskAttemptState getTaskAttemptState();
  public abstract float getProgress();
  public abstract long getStartTime();
  public abstract long getFinishTime();
  public abstract Counters getCounters();
  public abstract String getDiagnosticInfo();
  public abstract String getStateString();
  public abstract String getNodeManagerHost();
  public abstract int getNodeManagerPort();
  public abstract int getNodeManagerHttpPort();
  public abstract ContainerId getContainerId();

  public abstract void setTaskAttemptId(TaskAttemptId taskAttemptId);
  public abstract void setTaskAttemptState(TaskAttemptState taskAttemptState);
  public abstract void setProgress(float progress);
  public abstract void setStartTime(long startTime);
  public abstract void setFinishTime(long finishTime);
  public abstract void setCounters(Counters counters);
  public abstract void setDiagnosticInfo(String diagnosticInfo);
  public abstract void setStateString(String stateString);
  public abstract void setNodeManagerHost(String nmHost);
  public abstract void setNodeManagerPort(int nmPort);
  public abstract void setNodeManagerHttpPort(int nmHttpPort);
  public abstract void setContainerId(ContainerId containerId);
  
}
