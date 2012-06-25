package org.apache.hadoop.realtime.app.counter;

import org.apache.hadoop.realtime.records.CounterGroup;

public class CounterGroupFactory {
  
  protected static <T extends Enum<T>> CounterGroup newFrameworkGroup(Class<T> cls){
    return new FrameworkCounterGroup<T>(cls);
  }
  
}
