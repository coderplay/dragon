package org.apache.hadoop.realtime.protocol.records;

import org.apache.hadoop.realtime.records.TaskAttemptId;

public interface FsErrorRequest {

  public abstract TaskAttemptId getTaskAttemptId();
  
  public abstract void setTaskAttemptId(TaskAttemptId attemptId);
  
  public abstract String getMessage();
  
  public abstract void setMessage(String message);
}
