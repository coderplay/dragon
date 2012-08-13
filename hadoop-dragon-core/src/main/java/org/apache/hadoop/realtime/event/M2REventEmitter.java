package org.apache.hadoop.realtime.event;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.UncheckedTimeoutException;

public class M2REventEmitter<KEY, VALUE> implements EventEmitter<KEY, VALUE> {

  private static final Log LOG = LogFactory.getLog(M2REventEmitter.class);
  private ShuffleServiceDelegate delegate;

  public M2REventEmitter() {
    delegate = new ShuffleServiceDelegate();
  }

  @Override
  public boolean emitEvent(Event<KEY, VALUE> event) throws IOException,
      InterruptedException {
    return delegate.emitEvent();
  }

  @Override
  public boolean
      emitEvent(Event<KEY, VALUE> event, long timeout, TimeUnit unit)
          throws IOException, InterruptedException {
    SimpleTimeLimiter st = new SimpleTimeLimiter();
    boolean result = false;
    try {
      result = st.callWithTimeout(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return delegate.emitEvent();
        }

      }, timeout, unit, true);
    } catch (UncheckedTimeoutException e) {
      LOG.error("EmitEvent overtime.", e);
    } catch (Exception e) {
      LOG.error("EmitEvent failed by NodeManager", e);
    }
    return result;
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

}
