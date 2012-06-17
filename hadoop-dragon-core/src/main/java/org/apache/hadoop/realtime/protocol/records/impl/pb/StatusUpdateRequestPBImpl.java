package org.apache.hadoop.realtime.protocol.records.impl.pb;

import org.apache.hadoop.realtime.protocol.records.StatusUpdateRequest;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.TaskReport;

public class StatusUpdateRequestPBImpl implements StatusUpdateRequest {

  @Override
  public TaskAttemptId getTaskAttemptId() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setTaskAttemptId(TaskAttemptId taskAttemptId) {
    // TODO Auto-generated method stub

  }

  @Override
  public TaskReport getTaskReport() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setTaskStatus(TaskReport taskReport) {
    // TODO Auto-generated method stub

  }

}
