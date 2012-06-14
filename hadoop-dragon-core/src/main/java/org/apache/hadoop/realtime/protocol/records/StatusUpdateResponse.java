package org.apache.hadoop.realtime.protocol.records;

public interface StatusUpdateResponse {
  public abstract boolean getResult();
  
  public abstract void setResult(boolean result);
}
