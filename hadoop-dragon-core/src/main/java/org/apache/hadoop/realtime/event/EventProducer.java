package org.apache.hadoop.realtime.event;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.realtime.job.TaskAttempt;

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
  public void initialize(TaskAttempt context) throws IOException, InterruptedException;

  /**
   * Read the next event.
   * 
   * @return true if a key/value pair was read
   * @throws IOException
   * @throws InterruptedException
   */
  public boolean nextEvent() throws IOException, InterruptedException;

  /**
   * Get the current event.
   * 
   * @return the object that was read
   * @throws IOException
   * @throws InterruptedException
   */
  public Event<KEY,VALUE> getCurrentEvent() throws IOException, InterruptedException;


  /**
   * Close the record reader.
   */
  public void close() throws IOException;
}
