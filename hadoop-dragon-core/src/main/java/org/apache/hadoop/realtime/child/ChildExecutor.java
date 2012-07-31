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
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.realtime.DragonConfig;
import org.apache.hadoop.realtime.DragonJobConfig;
import org.apache.hadoop.realtime.app.counter.CountersManager;
import org.apache.hadoop.realtime.app.counter.FileSystemCounter;
import org.apache.hadoop.realtime.app.counter.TaskCounter;
import org.apache.hadoop.realtime.conf.DragonConfiguration;
import org.apache.hadoop.realtime.records.ChildExecutionContext;
import org.apache.hadoop.realtime.records.Counter;
import org.apache.hadoop.realtime.records.Counters;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.TaskAttemptReport;
import org.apache.hadoop.realtime.records.TaskType;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin.ProcResourceValues;

/**
 */
abstract class ChildExecutor {

  private static final Log LOG = LogFactory.getLog(ChildExecutor.class);
  private final static int MAX_RETRIES = 10;

  private TaskAttemptReport attemptReport;
  private TaskAttemptId taskAttemptId;
  protected ChildExecutionContext context;
  private TaskType taskType;

  private long initCpuCumulativeTime = 0;

  protected final CountersManager counterManager = new CountersManager();
  protected Counters counters = counterManager.getCounters();

  protected GcTimeUpdater gcUpdater;
  private ResourceCalculatorPlugin resourceCalculator = null;
  private AtomicBoolean taskDone = new AtomicBoolean(false);

  /** The number of milliseconds between progress reports. */
  public static final int PROGRESS_INTERVAL = 3000;

  public void initialize(DragonConfiguration conf, JobId id)
      throws IOException, ClassNotFoundException, InterruptedException {
    Class<? extends ResourceCalculatorPlugin> clazz =
        conf.getClass(DragonConfig.RESOURCE_CALCULATOR_PLUGIN, null,
            ResourceCalculatorPlugin.class);
    resourceCalculator =
        ResourceCalculatorPlugin.getResourceCalculatorPlugin(clazz, conf);
    LOG.info(" Using ResourceCalculatorPlugin : " + resourceCalculator);
    if (resourceCalculator != null) {
      initCpuCumulativeTime =
          resourceCalculator.getProcResourceValues().getCumulativeCpuTime();
    }
  }

  public ChildExecutor(ChildExecutionContext context) {
    this.context = context;
    this.taskAttemptId = context.getTaskAttemptId();
    this.taskType = context.getTaskType();
    attemptReport = Records.newRecord(TaskAttemptReport.class);
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  protected class TaskReporter implements Runnable {
    private ChildServiceDelegate delegate;
    private Thread pingThread = null;
    private boolean done = true;
    private Object lock = new Object();

    /**
     * flag that indicates whether progress update needs to be sent to parent.
     * If true, it has been set. If false, it has been reset. Using
     * AtomicBoolean since we need an atomic read & reset method.
     */
    private AtomicBoolean progressFlag = new AtomicBoolean(false);

    TaskReporter(ChildServiceDelegate delegate) {
      this.delegate = delegate;
    }

    // getters and setters for flag
    void setProgressFlag() {
      progressFlag.set(true);
    }

    boolean resetProgressFlag() {
      return progressFlag.getAndSet(false);
    }

    /**
     * The communication thread handles communication with the parent (Task
     * Tracker). It sends progress updates if progress has been made or if the
     * task needs to let the parent know that it's alive. It also pings the
     * parent to see if it's alive.
     */
    public void run() {
      final int MAX_RETRIES = 3;
      int remainingRetries = MAX_RETRIES;
      // get current flag value and reset it as well
      boolean sendProgress = resetProgressFlag();
      while (!taskDone.get()) {
        synchronized (lock) {
          done = false;
        }
        try {
          boolean taskFound = true; // whether TT knows about this task
          // sleep for a bit
          synchronized (lock) {
            if (taskDone.get()) {
              break;
            }
            lock.wait(PROGRESS_INTERVAL);
          }
          if (taskDone.get()) {
            break;
          }
          if (sendProgress) {
            updateCounters();
            attemptReport.setCounters(counters);
            delegate.statusUpdate(taskAttemptId, attemptReport);
          } else
            // send ping
            taskFound = delegate.ping(taskAttemptId);

          // if Task Tracker is not aware of our task ID (probably because it
          // died and
          // came back up), kill ourselves
          if (!taskFound) {
            LOG.warn("Parent died.  Exiting " + taskAttemptId);
            resetDoneFlag();
            System.exit(66);
          }
          remainingRetries = MAX_RETRIES;
        } catch (Throwable t) {
          LOG.info("Communication exception: "
              + StringUtils.stringifyException(t));
          remainingRetries -= 1;
          if (remainingRetries == 0) {
            ReflectionUtils.logThreadInfo(LOG, "Communication exception", 0);
            LOG.warn("Last retry, killing " + taskAttemptId);
            resetDoneFlag();
            System.exit(65);
          }
        }
      }
      // Notify that we are done with the work
      resetDoneFlag();
    }

    void resetDoneFlag() {
      synchronized (lock) {
        done = true;
        lock.notify();
      }
    }

    public void startCommunicationThread() {
      if (pingThread == null) {
        pingThread = new Thread(this, "communication thread");
        pingThread.setDaemon(true);
        pingThread.start();
      }
    }

    public void stopCommunicationThread() throws InterruptedException {
      if (pingThread != null) {
        // Intent of the lock is to not send an interupt in the middle of an
        // umbilical.ping or umbilical.statusUpdate
        synchronized (lock) {
          // Interrupt if sleeping. Otherwise wait for the RPC call to return.
          lock.notify();
        }

        synchronized (lock) {
          while (!done) {
            lock.wait();
          }
        }
        pingThread.interrupt();
        pingThread.join();
      }
    }
  }

  /**
   * Update resource information counters
   */
  void updateResourceCounters() {
    // Update generic resource counters
    updateHeapUsageCounter();

    // Updating resources specified in ResourceCalculatorPlugin
    if (resourceCalculator == null) {
      return;
    }
    ProcResourceValues res = resourceCalculator.getProcResourceValues();
    long cpuTime = res.getCumulativeCpuTime();
    long pMem = res.getPhysicalMemorySize();
    long vMem = res.getVirtualMemorySize();
    // Remove the CPU time consumed previously by JVM reuse
    cpuTime -= initCpuCumulativeTime;
    counters.getCounter(TaskCounter.CPU_MILLISECONDS).setValue(cpuTime);
    counters.getCounter(TaskCounter.PHYSICAL_MEMORY_BYTES).setValue(pMem);
    counters.getCounter(TaskCounter.VIRTUAL_MEMORY_BYTES).setValue(vMem);
  }

  /**
   * An updater that tracks the amount of time this task has spent in GC.
   */
  class GcTimeUpdater {
    private long lastGcMillis = 0;
    private List<GarbageCollectorMXBean> gcBeans = null;

    public GcTimeUpdater() {
      this.gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
      getElapsedGc(); // Initialize 'lastGcMillis' with the current time spent.
    }

    /**
     * @return the number of milliseconds that the gc has used for CPU since the
     *         last time this method was called.
     */
    protected long getElapsedGc() {
      long thisGcMillis = 0;
      for (GarbageCollectorMXBean gcBean : gcBeans) {
        thisGcMillis += gcBean.getCollectionTime();
      }

      long delta = thisGcMillis - lastGcMillis;
      this.lastGcMillis = thisGcMillis;
      return delta;
    }

    /**
     * Increment the gc-elapsed-time counter.
     */
    public void incrementGcCounter() {
      if (null == counters) {
        return; // nothing to do.
      }

      Counter gcCounter = counters.getCounter(TaskCounter.GC_TIME_MILLIS);
      if (null != gcCounter) {
        gcCounter.increment(getElapsedGc());
      }
    }
  }

  /**
   * An updater that tracks the last number reported for a given file system and
   * only creates the counters when they are needed.
   */
  class FileSystemStatisticUpdater {
    private List<FileSystem.Statistics> stats;
    private Counter readBytesCounter, writeBytesCounter, readOpsCounter,
        largeReadOpsCounter, writeOpsCounter;
    private String scheme;

    FileSystemStatisticUpdater(List<FileSystem.Statistics> stats, String scheme) {
      this.stats = stats;
      this.scheme = scheme;
    }

    void updateCounters() {
      if (readBytesCounter == null) {
        readBytesCounter =
            counterManager.findCounter(scheme, FileSystemCounter.BYTES_READ);
      }
      if (writeBytesCounter == null) {
        writeBytesCounter =
            counterManager.findCounter(scheme, FileSystemCounter.BYTES_WRITTEN);
      }
      if (readOpsCounter == null) {
        readOpsCounter =
            counterManager.findCounter(scheme, FileSystemCounter.READ_OPS);
      }
      if (largeReadOpsCounter == null) {
        largeReadOpsCounter =
            counterManager
                .findCounter(scheme, FileSystemCounter.LARGE_READ_OPS);
      }
      if (writeOpsCounter == null) {
        writeOpsCounter =
            counterManager.findCounter(scheme, FileSystemCounter.WRITE_OPS);
      }
      long readBytes = 0;
      long writeBytes = 0;
      long readOps = 0;
      long largeReadOps = 0;
      long writeOps = 0;
      for (FileSystem.Statistics stat : stats) {
        readBytes = readBytes + stat.getBytesRead();
        writeBytes = writeBytes + stat.getBytesWritten();
        readOps = readOps + stat.getReadOps();
        largeReadOps = largeReadOps + stat.getLargeReadOps();
        writeOps = writeOps + stat.getWriteOps();
      }
      readBytesCounter.setValue(readBytes);
      writeBytesCounter.setValue(writeBytes);
      readOpsCounter.setValue(readOps);
      largeReadOpsCounter.setValue(largeReadOps);
      writeOpsCounter.setValue(writeOps);
    }
  }

  /**
   * A Map where Key-> URIScheme and value->FileSystemStatisticUpdater
   */
  private Map<String, FileSystemStatisticUpdater> statisticUpdaters =
      new HashMap<String, FileSystemStatisticUpdater>();

  private synchronized void updateCounters() {
    Map<String, List<FileSystem.Statistics>> map =
        new HashMap<String, List<FileSystem.Statistics>>();
    for (Statistics stat : FileSystem.getAllStatistics()) {
      String uriScheme = stat.getScheme();
      if (map.containsKey(uriScheme)) {
        List<FileSystem.Statistics> list = map.get(uriScheme);
        list.add(stat);
      } else {
        List<FileSystem.Statistics> list =
            new ArrayList<FileSystem.Statistics>();
        list.add(stat);
        map.put(uriScheme, list);
      }
    }
    for (Map.Entry<String, List<FileSystem.Statistics>> entry : map.entrySet()) {
      FileSystemStatisticUpdater updater =
          statisticUpdaters.get(entry.getKey());
      if (updater == null) {// new FileSystem has been found in the cache
        updater =
            new FileSystemStatisticUpdater(entry.getValue(), entry.getKey());
        statisticUpdaters.put(entry.getKey(), updater);
      }
      updater.updateCounters();
    }

    gcUpdater.incrementGcCounter();
    updateResourceCounters();
  }

  /**
   * Updates the {@link TaskCounter#COMMITTED_HEAP_BYTES} counter to reflect the
   * current total committed heap space usage of this JVM.
   */
  private void updateHeapUsageCounter() {
    long currentHeapUsage = Runtime.getRuntime().totalMemory();
    counters.getCounter(TaskCounter.COMMITTED_HEAP_BYTES).setValue(
        currentHeapUsage);
  }

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

  protected void execute(final Configuration conf,
      final ChildServiceDelegate delegate) throws IOException,
      InterruptedException, ClassNotFoundException {
    Class<?> inputKey = null;
    Class<?> inputValue = null;
    Class<?> outputKey = null;
    Class<?> outputValue = null;
    if (taskType.equals(TaskType.MAP)) {
      inputKey = 
          conf.getClass(DragonJobConfig.JOB_MAP_INPUT_KEY_CLASS,Object.class);
      inputValue =
          conf.getClass(DragonJobConfig.JOB_MAP_INPUT_VALUE_CLASS,Object.class);
      outputKey = 
          conf.getClass(DragonJobConfig.JOB_MAP_OUTPUT_KEY_CLASS,Object.class);
      outputValue =
          conf.getClass(DragonJobConfig.JOB_MAP_OUTPUT_VALUE_CLASS,Object.class);

    } else if (taskType.equals(TaskType.REDUCE)) {
      inputKey =
          conf.getClass(DragonJobConfig.JOB_REDUCE_INPUT_KEY_CLASS,Object.class);
      inputValue =
          conf.getClass(DragonJobConfig.JOB_REDUCE_INPUT_VALUE_CLASS,Object.class);
      outputKey =
          conf.getClass(DragonJobConfig.JOB_REDUCE_OUTPUT_KEY_CLASS,Object.class);
      outputValue =
          conf.getClass(DragonJobConfig.JOB_REDUCE_OUTPUT_VALUE_CLASS,Object.class);
    }
    execute(conf, inputKey, inputValue, outputKey, outputValue);
  }

  protected abstract <KEYIN, VALUEIN, KEYOUT, VALUEOUT> void execute(
      Configuration conf, Class<KEYIN> keyInClass, Class<VALUEIN> valueInClass,
      Class<KEYOUT> keyOutClass, Class<VALUEOUT> valueOutClass)
      throws IOException, InterruptedException;

  public void statusUpdate(ChildServiceDelegate delegate) throws IOException {
    int retries = MAX_RETRIES;
    while (true) {
      try {
        if (!delegate.statusUpdate(taskAttemptId, attemptReport)) {
          LOG.warn("Parent died.  Exiting " + taskAttemptId);
          System.exit(66);
        }
        return;
      } catch (IOException ie) {
        LOG.warn("Failure sending status update: "
            + StringUtils.stringifyException(ie));
        if (--retries == 0) {
          throw ie;
        }
      }
    }
  }

}
