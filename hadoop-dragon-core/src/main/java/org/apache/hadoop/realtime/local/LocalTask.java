package org.apache.hadoop.realtime.local;

import java.util.Map;

import org.apache.hadoop.realtime.job.Task;
import org.apache.hadoop.realtime.job.TaskAttempt;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.TaskId;
import org.apache.hadoop.realtime.records.TaskReport;

public class LocalTask implements Task{

  @Override
  public TaskId getId() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Map<TaskAttemptId, TaskAttempt> getAttempts() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TaskAttempt getAttempt(TaskAttemptId attemptId) {
    // TODO Auto-generated method stub
    return null;
  }
  
  public void run(){
    
  }

  @Override
  public TaskReport getReport() {
    // TODO Auto-generated method stub
    return null;
  }

}
