package org.apache.hadoop.realtime.job;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.realtime.protocol.DragonChildProtocol;
import org.apache.hadoop.realtime.records.Counters;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.TaskId;
import org.apache.hadoop.realtime.records.TaskInChild;
import org.apache.hadoop.realtime.records.TaskReport;
import org.apache.hadoop.realtime.records.TaskState;

public class TaskInChildImpl extends TaskInChild {

  private static final Log LOG = LogFactory
      .getLog(TaskInChildImpl.class);
  private Path workingPath;
  
  public TaskInChildImpl(){
    
  }
  @Override
  public TaskId getID() {
    // TODO Auto-generated method stub
    return null;
  }
  
  public void setWorkingPath(Path path){
    this.workingPath=path;
  }
  
  public void run(Configuration conf , DragonChildProtocol childProtocol){
      LOG.error("Task is start!");
    try {
      while (true) {
        Thread.sleep(5000);
        LOG.error("Task is running!");
      }
    } catch (InterruptedException e) {
      LOG.error("Task is end!", e);
    }
  }
  @Override
  public void setID(TaskId taskId) {
    // TODO Auto-generated method stub
    
  }
  @Override
  public TaskReport getReport() {
    // TODO Auto-generated method stub
    return null;
  }
  @Override
  public TaskState getState() {
    // TODO Auto-generated method stub
    return null;
  }
  @Override
  public Counters getCounters() {
    // TODO Auto-generated method stub
    return null;
  }
  @Override
  public String getLabel() {
    // TODO Auto-generated method stub
    return null;
  }
  @Override
  public Map<TaskAttemptId, TaskAttempt> getAttempts() {
    // TODO Auto-generated method stub
    return null;
  }
  @Override
  public TaskAttempt getAttempt(TaskAttemptId attemptID) {
    // TODO Auto-generated method stub
    return null;
  }

}
