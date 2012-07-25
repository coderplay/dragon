package org.apache.hadoop.realtime.event;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.realtime.records.ChildExecutionContext;

/**
 * 
 */
public interface EventProducer<KEY, VALUE> extends Closeable {

  /**
   * Called once at initialization.
   * 
   * @param context the information about the task
   * @throws IOException
   * @throws InterruptedException
   */
  public void initialize(ChildExecutionContext context) throws IOException,
      InterruptedException;

  /**
   * Read the next event.
   * 
   * @return true if a key/value pair was read
   * @throws IOException
   * @throws InterruptedException
   */
  public Event<KEY, VALUE> pollEvent() throws IOException, InterruptedException;

  /**
   * Read the next event.
   * 
   * @return true if a key/value pair was read
   * @throws IOException
   * @throws InterruptedException
   */
  public Event<KEY, VALUE> pollEvent(long timeout, TimeUnit unit)
      throws IOException, InterruptedException;

  /**
   * Close the event producer.
   */
  public void close() throws IOException;
}
