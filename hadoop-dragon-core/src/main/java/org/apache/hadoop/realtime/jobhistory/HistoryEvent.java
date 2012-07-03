package org.apache.hadoop.realtime.jobhistory;

import org.apache.hadoop.yarn.event.AbstractEvent;

/**
 * class description goes here.
 *
 * @author xiaofeng_metis
 */
public class HistoryEvent extends AbstractEvent<EventType> {
  public HistoryEvent(EventType eventType) {
    super(eventType);
  }

}
