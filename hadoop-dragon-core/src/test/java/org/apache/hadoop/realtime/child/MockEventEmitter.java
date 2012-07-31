package org.apache.hadoop.realtime.child;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.realtime.event.Event;
import org.apache.hadoop.realtime.event.EventEmitter;

public class MockEventEmitter<KEY, VALUE> implements EventEmitter<KEY, VALUE> {

  private static final Log LOG = LogFactory.getLog(MockEventEmitter.class);
  
  @Override
  public boolean emitEvent(Event<KEY, VALUE> event) throws IOException,
      InterruptedException {
    LOG.debug("emit event called.");
    return false;
  }

  @Override
  public boolean
      emitEvent(Event<KEY, VALUE> event, long timeout, TimeUnit unit)
          throws IOException, InterruptedException {
    LOG.debug("emit event with time out called.");
    return false;
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub
    
  }

}
