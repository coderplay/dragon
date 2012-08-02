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

package org.apache.hadoop.realtime.webapp.dao;

import org.apache.hadoop.realtime.job.TaskAttempt;
import org.apache.hadoop.realtime.records.CounterGroup;
import org.apache.hadoop.realtime.records.Counters;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import java.util.ArrayList;

@XmlRootElement(name = "jobTaskAttemptCounters")
@XmlAccessorType(XmlAccessType.FIELD)
public class JobTaskAttemptCounterInfo {

  @XmlTransient
  protected Counters total = null;

  protected String id;
  protected ArrayList<TaskCounterGroupInfo> taskAttemptCounterGroup;

  public JobTaskAttemptCounterInfo() {
  }

  public JobTaskAttemptCounterInfo(TaskAttempt taskattempt) {

    this.id = taskattempt.getID().toString();
    total = taskattempt.getCounters();
    taskAttemptCounterGroup = new ArrayList<TaskCounterGroupInfo>();
    if (total != null) {
      for (CounterGroup g : total.getAllCounterGroups().values()) {
        if (g != null) {
          TaskCounterGroupInfo cginfo = new TaskCounterGroupInfo(g.getName(), g);
          if (cginfo != null) {
            taskAttemptCounterGroup.add(cginfo);
          }
        }
      }
    }
  }
}
