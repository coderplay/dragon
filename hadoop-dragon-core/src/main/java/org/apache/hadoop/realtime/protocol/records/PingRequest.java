package org.apache.hadoop.realtime.protocol.records;

import org.apache.hadoop.realtime.records.TaskAttemptId;


public interface PingRequest {

  public abstract TaskAttemptId getTaskAttemptId();
  
  public abstract void setTaskAttemptId(TaskAttemptId taskAttemptId);
}
