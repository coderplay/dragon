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

import org.apache.hadoop.realtime.records.Counter;
import org.apache.hadoop.realtime.records.CounterGroup;
import org.apache.hadoop.realtime.records.impl.pb.CounterGroupPBImpl;
import org.apache.hadoop.realtime.util.ResourceBundles;

public abstract class AbstractCounterGroup extends CounterGroupPBImpl implements
    CounterGroup {

  private final String name;
  private String displayName;
  private final Limits limits;

  public AbstractCounterGroup(String name, String displayName, Limits limits) {
    this.name = name;
    this.displayName = displayName;
    this.limits = limits;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public synchronized String getDisplayName() {
    return displayName;
  }

  @Override
  public synchronized void setDisplayName(String displayName) {
    this.displayName = displayName;
  }

  protected abstract Counter newCounter(String counterName, String displayName,
      long value);

  @Override
  public synchronized Counter getCounter(String key) {
    Counter counter = super.getCounter(key);
    if (counter == null) {
      String localized = ResourceBundles.getCounterName(getName(), key, key);
      counter = newCounter(key, localized, 0);
      setCounter(key, counter);
    }
    return counter;
  }

  @Override
  public void setCounter(String key, Counter value) {
    super.setCounter(key, value);
    limits.incrCounters();
  }
  
  public synchronized Counter
      findCounter(String counterName, String displayName) {
    String saveName = Limits.filterCounterName(counterName);
    Counter counter = getCounter(saveName);
    return counter;
  }

  @Override
  public void incrAllCounters(CounterGroup rightGroup) {
    try {
      for (Counter right : rightGroup.getAllCounters().values()) {
        Counter left = findCounter(right.getName(), right.getDisplayName());
        left.increment(right.getValue());
      }
    } catch (LimitExceededException e) {
      getAllCounters().clear();
      throw e;
    }
  }
}
