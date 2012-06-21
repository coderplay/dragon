package org.apache.hadoop.realtime.protocol.records;

import org.apache.hadoop.realtime.records.ChildExecutionContext;

public interface GetTaskResponse {

  public abstract ChildExecutionContext getTask();
  
  public abstract void setTask(ChildExecutionContext task);
}
