package org.apache.hadoop.realtime.protocol.records;

import org.apache.hadoop.realtime.records.TaskInChild;

public interface GetTaskResponse {

  public abstract TaskInChild getTask();
  
  public abstract void setTask(TaskInChild task);
}
