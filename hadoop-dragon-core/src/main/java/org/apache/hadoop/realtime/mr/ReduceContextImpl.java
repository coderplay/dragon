package org.apache.hadoop.realtime.mr;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
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

  private Counter inputValueCounter;
  private Counter outputValueCounter;

  private EventProducer<KEYIN, VALUEIN> producer;
  private EventEmitter<KEYOUT, VALUEOUT> emitter;

  public ReduceContextImpl(Configuration conf, ChildExecutionContext context,
      Counter inputValueCounter, Counter outputValueCounter,
      EventProducer<KEYIN, VALUEIN> producer,
      EventEmitter<KEYOUT, VALUEOUT> emitter) throws IOException {
    this.context = context;
    this.conf = new DragonConfiguration(conf);

    this.producer = producer;
    this.emitter = emitter;

    this.inputValueCounter = inputValueCounter;
    this.outputValueCounter = outputValueCounter;

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
    producer.pollEvent();
    inputValueCounter.increment(1);
    return null;
  }

  @Override
  public Event<KEYIN, VALUEIN> pollEvent(long timeout, TimeUnit unit)
      throws IOException, InterruptedException {
    producer.pollEvent(timeout, unit);
    inputValueCounter.increment(1);
    return null;
  }

  @Override
  public boolean emitEvent(Event<KEYOUT, VALUEOUT> event) throws IOException,
      InterruptedException {
    emitter.emitEvent(event);
    outputValueCounter.increment(1);
    return false;
  }

  @Override
  public boolean emitEvent(Event<KEYOUT, VALUEOUT> event, long timeout,
      TimeUnit unit) throws IOException, InterruptedException {
    emitter.emitEvent(event, timeout, unit);
    outputValueCounter.increment(1);
    return false;
  }

}
