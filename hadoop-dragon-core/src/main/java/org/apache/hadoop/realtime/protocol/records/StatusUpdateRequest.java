package org.apache.hadoop.realtime.protocol.records;

import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.TaskReport;

public interface StatusUpdateRequest {

  public abstract TaskAttemptId getTaskAttemptId();
  
  public abstract void setTaskAttemptId(TaskAttemptId taskAttemptId);
  
  public abstract TaskReport getTaskReport();
  
  public abstract void setTaskStatus(TaskReport taskReport);
  
}
