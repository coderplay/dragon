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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.realtime.records.Counter;
import org.apache.hadoop.realtime.records.CounterGroup;
import org.apache.hadoop.realtime.records.impl.pb.CounterGroupPBImpl;
import org.apache.hadoop.realtime.records.impl.pb.CounterPBImpl;
import org.apache.hadoop.realtime.util.ResourceBundles;

public class FrameworkCounterGroup<T extends Enum<T>> extends
    CounterGroupPBImpl implements CounterGroup {

  private static final Log LOG = LogFactory.getLog(FrameworkCounterGroup.class);

  private final Class<T> enumClass;
  private String displayName;

  @InterfaceAudience.Private
  public class FrameworkCounter extends CounterPBImpl implements Counter {

    private T key;

    public FrameworkCounter(T key) {
      this.key = key;
      setValue(0);
    }

    @Override
    public String getName() {
      return key.name();
    }

    @Override
    public String getDisplayName() {
      return localizeCounterName(getName());
    }

    @Override
    public void increment(long incr) {
      setValue(getValue() + incr);
    }
  }

  public FrameworkCounterGroup(Class<T> enumClass) {
    this.enumClass = enumClass;
  }

  @Override
  public String getName() {
    return enumClass.getName();
  }

  @Override
  public String getDisplayName() {
    if (displayName == null) {
      displayName = ResourceBundles.getCounterGroupName(getName(), getName());
    }
    return displayName;
  }

  @Override
  public synchronized Counter getCounter(String key) {
    Counter counter = super.getCounter(key);
    if (counter == null) {
      counter = new FrameworkCounter(valueOf(key));
      setCounter(key, counter);
    }
    return counter;
  }

  private String localizeCounterName(String counterName) {
    return ResourceBundles.getCounterName(getName(), counterName, counterName);
  }

  private T valueOf(String name) {
    return Enum.valueOf(enumClass, name);
  }

}
