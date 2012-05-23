package org.apache.hadoop.realtime.event;

import java.io.IOException;

import org.apache.hadoop.realtime.job.TaskAttempt;

public abstract class EventProcessor<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {

  public abstract class Context
    implements TaskAttempt {
  }

  /**
   * Called once at the beginning of the task.
   */
  protected void setup(Context context
                       ) throws IOException, InterruptedException {
    // NOTHING
  }

  protected abstract void execute(KEYIN key, VALUEIN value, Context context) throws IOException;

  /**
   * Called once at the end of the task.
   */
  protected void cleanup(Context context
                         ) throws IOException, InterruptedException {
    // NOTHING
  }
}
