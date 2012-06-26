/*
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
package org.apache.hadoop.realtime.app.counter;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.realtime.FileSystemCounter;
import org.apache.hadoop.realtime.TaskCounter;
import org.apache.hadoop.realtime.job.JobCounter;
import org.apache.hadoop.realtime.records.Counter;
import org.apache.hadoop.realtime.records.Counters;
import org.apache.hadoop.realtime.util.DragonBuilderUtils;

import com.google.common.collect.Maps;

public class CountersInAppMaster {

  protected static final Log LOG = LogFactory.getLog("dragon.Counters");

  private Map<Enum<?>, Counter> cache = Maps.newIdentityHashMap();

  private final Counters counters = DragonBuilderUtils.newCounters();

  {
    addFrameworkGroup(JobCounter.class);
    addFrameworkGroup(TaskCounter.class);
  }

  private <T extends Enum<T>> void addFrameworkGroup(final Class<T> cls) {
    counters.setCounterGroup(cls.getName(),
        CounterGroupFactory.newFrameworkGroup(cls));
  }

  public synchronized Counter findCounter(String scheme, FileSystemCounter key) {
    FileSystemCounterGroup group = (FileSystemCounterGroup) counters.getCounterGroup(FileSystemCounter.class.getName());
    return group.findCounter(scheme, key);
  }
  
  public synchronized Counter getCounter(Enum<?> key) {
    Counter counter = cache.get(key);
    if (counter == null) {
      counter = counters.getCounter(key);
      cache.put(key, counter);
    }
    return counter;
  }
  
  public Counters getCounters(){
    return counters;
  }
}
