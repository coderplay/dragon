package org.apache.hadoop.realtime.jobhistory;

/**
 * Interface for event wrapper classes. we use kryo serialize it
 * adding constructors and accessor methods.
 */
public interface HistoryEvent {

  /** Return this event's type. */
  EventType getEventType();

}

