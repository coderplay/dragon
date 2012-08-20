package org.apache.hadoop.realtime.child;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.realtime.event.Event;
import org.apache.hadoop.realtime.event.EventProducer;
import org.apache.hadoop.realtime.records.ChildExecutionContext;

public class MockEventProducer<KEY, VALUE> implements EventProducer<KEY, VALUE> {

  private static final Log LOG = LogFactory.getLog(MockEventEmitter.class);
  @Override
  public void initialize(ChildExecutionContext context) throws IOException,
      InterruptedException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public Event<KEY, VALUE> pollEvent() throws IOException, InterruptedException {
    LOG.debug("poll event called.");
    return (Event<KEY, VALUE>) new Event<Text, IntWritable>(new Text(),
        new IntWritable(1));
  }

  @Override
  public Event<KEY, VALUE> pollEvent(long timeout, TimeUnit unit)
      throws IOException, InterruptedException {
    LOG.debug("poll event with timeout called.");
    return null;
  }

  @Override
  public void close() throws IOException {
  }

}
