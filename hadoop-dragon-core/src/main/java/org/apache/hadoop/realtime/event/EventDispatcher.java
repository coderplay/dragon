package org.apache.hadoop.realtime.event;


public interface EventDispatcher<KEY, VALUE> {
  /**
   * Dispatch event using stream name. Partitioners may be used to partition
   * the event, possibly based on a pre-determined set of fixed named keys.
   * 
   * @param streamName
   *            name of stream to dispatch on
   * @param key
   *            object to dispatch
   */
  void dispatch(String streamName, KEY key, VALUE value);

}
