package org.apache.hadoop.realtime.protocol.records;


public interface GetTaskRequest {

  public abstract String getContainerId();
  
  public abstract void setContainerId(String containerId);
}
