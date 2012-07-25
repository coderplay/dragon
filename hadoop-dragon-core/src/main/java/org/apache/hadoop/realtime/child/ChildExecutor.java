/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.realtime.child;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.realtime.DragonJobConfig;
import org.apache.hadoop.realtime.conf.DragonConfiguration;
import org.apache.hadoop.realtime.event.Event;
import org.apache.hadoop.realtime.event.EventEmitter;
import org.apache.hadoop.realtime.event.EventProducer;
import org.apache.hadoop.realtime.mr.MapContext;
import org.apache.hadoop.realtime.mr.Mapper;
import org.apache.hadoop.realtime.protocol.DragonChildProtocol;
import org.apache.hadoop.realtime.records.ChildExecutionContext;
import org.apache.hadoop.realtime.records.Counters;
import org.apache.hadoop.realtime.records.TaskType;
import org.apache.hadoop.util.ReflectionUtils;

/**
 */
abstract class ChildExecutor {
  // @InterfaceAudience.Private
  // @InterfaceStability.Unstable
  // protected class TaskReporter implements StatusReporter, Runnable {
  // private DragonChildProtocol umbilical;;
  // private DragonConfiguration conf;
  // private Progress taskProgress;
  // private Thread pingThread = null;
  // private boolean done = true;
  // private Object lock = new Object();
  //
  // TaskReporter(Progress taskProgress, DragonChildProtocol umbilical) {
  // this.umbilical = umbilical;
  // this.taskProgress = taskProgress;
  // }
  //
  //
  // public void setStatus(String status) {
  // taskProgress.setStatus(normalizeStatus(status, conf));
  // }
  //
  // public Counters.Counter getCounter(String group, String name) {
  // Counters.Counter counter = null;
  // if (counters != null) {
  // counter = counters.findCounter(group, name);
  // }
  // return counter;
  // }
  //
  // public Counters.Counter getCounter(Enum<?> name) {
  // return counters == null ? null : counters.findCounter(name);
  // }
  //
  // public void incrCounter(Enum key, long amount) {
  // if (counters != null) {
  // counters.incrCounter(key, amount);
  // }
  // }
  //
  // public void incrCounter(String group, String counter, long amount) {
  // if (counters != null) {
  // counters.incrCounter(group, counter, amount);
  // }
  // if (skipping
  // && SkipBadRecords.COUNTER_GROUP.equals(group)
  // && (SkipBadRecords.COUNTER_MAP_PROCESSED_RECORDS.equals(counter) ||
  // SkipBadRecords.COUNTER_REDUCE_PROCESSED_GROUPS
  // .equals(counter))) {
  // // if application reports the processed records, move the
  // // currentRecStartIndex to the next.
  // // currentRecStartIndex is the start index which has not yet been
  // // finished and is still in task's stomach.
  // for (int i = 0; i < amount; i++) {
  // currentRecStartIndex = currentRecIndexIterator.next();
  // }
  // }
  // setProgressFlag();
  // }
  //
  // /**
  // * The communication thread handles communication with the parent (Task
  // * Tracker). It sends progress updates if progress has been made or if the
  // * task needs to let the parent know that it's alive. It also pings the
  // * parent to see if it's alive.
  // */
  // public void run() {
  // final int MAX_RETRIES = 3;
  // int remainingRetries = MAX_RETRIES;
  // // get current flag value and reset it as well
  // boolean sendProgress = resetProgressFlag();
  // while (!taskDone.get()) {
  // synchronized (lock) {
  // done = false;
  // }
  // try {
  // boolean taskFound = true; // whether TT knows about this task
  // // sleep for a bit
  // synchronized (lock) {
  // if (taskDone.get()) {
  // break;
  // }
  // lock.wait(PROGRESS_INTERVAL);
  // }
  // if (taskDone.get()) {
  // break;
  // }
  //
  // if (sendProgress) {
  // // we need to send progress update
  // updateCounters();
  // taskStatus.statusUpdate(taskProgress.get(),
  // taskProgress.toString(), counters);
  // taskFound = umbilical.statusUpdate(taskId, taskStatus);
  // taskStatus.clearStatus();
  // } else {
  // // send ping
  // taskFound = umbilical.ping(taskId);
  // }
  //
  // // if Task Tracker is not aware of our task ID (probably because it
  // // died and
  // // came back up), kill ourselves
  // if (!taskFound) {
  // LOG.warn("Parent died.  Exiting " + taskId);
  // resetDoneFlag();
  // System.exit(66);
  // }
  //
  // sendProgress = resetProgressFlag();
  // remainingRetries = MAX_RETRIES;
  // } catch (Throwable t) {
  // LOG.info("Communication exception: "
  // + StringUtils.stringifyException(t));
  // remainingRetries -= 1;
  // if (remainingRetries == 0) {
  // ReflectionUtils.logThreadInfo(LOG, "Communication exception", 0);
  // LOG.warn("Last retry, killing " + taskId);
  // resetDoneFlag();
  // System.exit(65);
  // }
  // }
  // }
  // // Notify that we are done with the work
  // resetDoneFlag();
  // }
  //
  // void resetDoneFlag() {
  // synchronized (lock) {
  // done = true;
  // lock.notify();
  // }
  // }
  //
  // public void startCommunicationThread() {
  // if (pingThread == null) {
  // pingThread = new Thread(this, "communication thread");
  // pingThread.setDaemon(true);
  // pingThread.start();
  // }
  // }
  //
  // public void stopCommunicationThread() throws InterruptedException {
  // if (pingThread != null) {
  // // Intent of the lock is to not send an interupt in the middle of an
  // // umbilical.ping or umbilical.statusUpdate
  // synchronized (lock) {
  // // Interrupt if sleeping. Otherwise wait for the RPC call to return.
  // lock.notify();
  // }
  //
  // synchronized (lock) {
  // while (!done) {
  // lock.wait();
  // }
  // }
  // pingThread.interrupt();
  // pingThread.join();
  // }
  // }
  // }

  /**
   * Gets a handle to the Statistics instance based on the scheme associated
   * with path.
   * 
   * @param path the path.
   * @param conf the configuration to extract the scheme from if not part of the
   *          path.
   * @return a Statistics instance, or null if none is found for the scheme.
   */
  protected static List<Statistics> getFsStatistics(Path path,
      Configuration conf) throws IOException {
    List<Statistics> matchedStats = new ArrayList<FileSystem.Statistics>();
    path = path.getFileSystem(conf).makeQualified(path);
    String scheme = path.toUri().getScheme();
    for (Statistics stats : FileSystem.getAllStatistics()) {
      if (stats.getScheme().equals(scheme)) {
        matchedStats.add(stats);
      }
    }
    return matchedStats;
  }

  protected <KEYIN, VALUEIN, KEYOUT, VALUEOUT> void execute(
      final Configuration conf, final DragonChildProtocol proxy,
      final ChildExecutionContext context) throws IOException,
      InterruptedException {
    // make a config helper so we can get the classes 
  }


}
