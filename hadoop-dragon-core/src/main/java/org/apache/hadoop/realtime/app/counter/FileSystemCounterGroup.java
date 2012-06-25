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

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.realtime.FileSystemCounter;
import org.apache.hadoop.realtime.records.Counter;
import org.apache.hadoop.realtime.records.CounterGroup;
import org.apache.hadoop.realtime.records.impl.pb.CounterGroupPBImpl;
import org.apache.hadoop.realtime.records.impl.pb.CounterPBImpl;
import org.apache.hadoop.realtime.util.ResourceBundles;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

public class FileSystemCounterGroup extends CounterGroupPBImpl implements
    CounterGroup {

  private static final Log LOG = LogFactory
      .getLog(FileSystemCounterGroup.class);

  static final int MAX_NUM_SCHEMES = 100;
  static final ConcurrentMap<String, String> schemes = Maps.newConcurrentMap();
  private static final Joiner NAME_JOINER = Joiner.on('_');
  private static final Joiner DISP_JOINER = Joiner.on(": ");

  private String displayName;

  @InterfaceAudience.Private
  public class FSCounter extends CounterPBImpl implements Counter {
    final String scheme;
    final FileSystemCounter key;

    public FSCounter(String scheme, FileSystemCounter ref) {
      this.scheme = scheme;
      key = ref;
      setValue(0);
    }

    @Override
    public String getName() {
      return NAME_JOINER.join(scheme, key.name());
    }

    @Override
    public String getDisplayName() {
      return DISP_JOINER.join(scheme, localizeCounterName(key.name()));
    }

    protected String localizeCounterName(String counterName) {
      return ResourceBundles.getCounterName(FileSystemCounter.class.getName(),
          counterName, counterName);
    }

    @Override
    public Counter getUnderlyingCounter() {
      return this;
    }
  }

  @Override
  public String getName() {
    return FileSystemCounter.class.getName();
  }

  @Override
  public String getDisplayName() {
    if (displayName == null) {
      displayName =
          ResourceBundles
              .getCounterGroupName(getName(), "File System Counters");
    }
    return displayName;
  }

  @Override
  public void setDisplayName(String displayName) {
    this.displayName = displayName;
  }
  
  @Override
  public synchronized Counter getCounter(String key) {
    Counter counter = super.getCounter(key);
    if (counter == null) {
      String[] pair = parseCounterName(key);
      counter = new FSCounter(pair[0], FileSystemCounter.valueOf(pair[1]));
      setCounter(key, counter);
    }
    return counter;
  }
  
  public synchronized Counter findCounter(String scheme, FileSystemCounter key) {
    final String canonicalScheme = checkScheme(scheme);
    Counter counter = getCounter(NAME_JOINER.join(canonicalScheme, key.name()));
    if(counter == null){
      counter = new FSCounter(canonicalScheme,key);
    }
    return counter;
  }
  
  private String[] parseCounterName(String counterName) {
    int schemeEnd = counterName.indexOf('_');
    if (schemeEnd < 0) {
      throw new IllegalArgumentException("bad fs counter name");
    }
    return new String[]{counterName.substring(0, schemeEnd),
                        counterName.substring(schemeEnd + 1)};
  }

  private String checkScheme(String scheme) {
    String fixed = scheme.toUpperCase(Locale.US);
    String interned = schemes.putIfAbsent(fixed, fixed);
    if (schemes.size() > MAX_NUM_SCHEMES) {
      // mistakes or abuses
      throw new IllegalArgumentException("too many schemes? "+ schemes.size() +
                                         " when process scheme: "+ scheme);
    }
    return interned == null ? fixed : interned;
  }
  
}
