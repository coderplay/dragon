package org.apache.hadoop.realtime.mr;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.realtime.app.counter.TaskCounter;
import org.apache.hadoop.realtime.child.ChildExecutor.TaskReporter;
import org.apache.hadoop.realtime.conf.DragonConfiguration;
import org.apache.hadoop.realtime.event.Event;
import org.apache.hadoop.realtime.event.EventEmitter;
import org.apache.hadoop.realtime.event.EventProducer;
import org.apache.hadoop.realtime.records.ChildExecutionContext;
import org.apache.hadoop.realtime.records.Counter;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.TaskType;

public class ReduceContextImpl<KEYIN, VALUEIN, KEYOUT, VALUEOUT> implements
    ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  private final ChildExecutionContext context;

  private final DragonConfiguration conf;
  private final TaskAttemptId attemptId;
  
  private TaskReporter reporter;

  private EventProducer<KEYIN, VALUEIN> producer;
  private EventEmitter<KEYOUT, VALUEOUT> emitter;

  public ReduceContextImpl(Configuration conf, ChildExecutionContext context,
      TaskReporter reporter,
      EventProducer<KEYIN, VALUEIN> producer,
      EventEmitter<KEYOUT, VALUEOUT> emitter) throws IOException {
    this.context = context;
    this.conf = new DragonConfiguration(conf);

    this.producer = producer;
    this.emitter = emitter;

    this.attemptId = context.getTaskAttemptId();

  }

  @Override
  public DragonConfiguration getConfiguration() {
    return conf;
  }

  @Override
  public TaskAttemptId getTaskAttemptId() {
    return attemptId;
  }

  @Override
  public int getPartition() {
    return context.getPartition();
  }

  @Override
  public String getUser() {
    return context.getUser();
  }

  @Override
  public TaskType getTaskType() {
    return TaskType.MAP;
  }

  @Override
  public Event<KEYIN, VALUEIN> pollEvent() throws IOException,
      InterruptedException {   
    reporter.incrCounter(TaskCounter.REDUCE_INPUT_RECORDS, 1);
    return producer.pollEvent();
  }

  @Override
  public Event<KEYIN, VALUEIN> pollEvent(long timeout, TimeUnit unit)
      throws IOException, InterruptedException { 
    reporter.incrCounter(TaskCounter.REDUCE_INPUT_RECORDS, 1);
    return producer.pollEvent(timeout, unit);
  }

  @Override
  public boolean emitEvent(Event<KEYOUT, VALUEOUT> event) throws IOException,
      InterruptedException {
    emitter.emitEvent(event);
    reporter.incrCounter(TaskCounter.REDUCE_OUTPUT_RECORDS, 1);
    return true;
  }

  @Override
  public boolean emitEvent(Event<KEYOUT, VALUEOUT> event, long timeout,
      TimeUnit unit) throws IOException, InterruptedException {
    emitter.emitEvent(event, timeout, unit);
    reporter.incrCounter(TaskCounter.REDUCE_OUTPUT_RECORDS, 1);
    return true;
  }

}
