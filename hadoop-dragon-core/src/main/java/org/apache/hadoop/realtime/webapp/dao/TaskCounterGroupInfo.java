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

import org.apache.hadoop.realtime.records.Counter;
import org.apache.hadoop.realtime.records.CounterGroup;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class TaskCounterGroupInfo {

  protected String counterGroupName;
  protected ArrayList<TaskCounterInfo> counter;

  public TaskCounterGroupInfo() {
  }

  public TaskCounterGroupInfo(String name, CounterGroup group) {
    this.counterGroupName = name;
    this.counter = new ArrayList<TaskCounterInfo>();

    for (Counter c : group.getAllCounters().values()) {
      TaskCounterInfo cinfo = new TaskCounterInfo(c.getName(), c.getValue());
      this.counter.add(cinfo);
    }
  }
}
