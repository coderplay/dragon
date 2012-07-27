package org.apache.hadoop.realtime.protocol.records;

import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.TaskAttemptReport;

public interface StatusUpdateRequest {

  public abstract TaskAttemptId getTaskAttemptId();
  
  public abstract void setTaskAttemptId(TaskAttemptId taskAttemptId);
  
  public abstract TaskAttemptReport getTaskAttemptReport();
  
  public abstract void setTaskStatus(TaskAttemptReport TaskAttemptReport);
  
}
