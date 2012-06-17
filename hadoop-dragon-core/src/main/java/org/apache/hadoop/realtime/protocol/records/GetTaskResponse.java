package org.apache.hadoop.realtime.protocol.records;

import org.apache.hadoop.realtime.job.Task;

public interface GetTaskResponse {

  public abstract Task getTask();
  
  public abstract void setTask(Task task);
}
