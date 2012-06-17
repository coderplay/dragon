package org.apache.hadoop.realtime.records;

import org.apache.hadoop.realtime.job.Task;


public abstract class TaskInChild implements Task{

  public abstract TaskId getID();
  public abstract void setID(TaskId taskId);
}
