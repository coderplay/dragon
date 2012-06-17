package org.apache.hadoop.realtime.protocol.records;

import org.apache.hadoop.yarn.api.records.ContainerId;

public interface GetTaskRequest {

  public abstract ContainerId getContainerId();
  
  public abstract void setContainerId(ContainerId containerId);
}
