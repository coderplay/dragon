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
package org.apache.hadoop.realtime;

import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.security.PrivilegedExceptionAction;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.realtime.conf.DragonConfiguration;
import org.apache.hadoop.realtime.mr.Mapper;
import org.apache.hadoop.realtime.mr.Reducer;
import org.apache.hadoop.realtime.records.CounterGroup;
import org.apache.hadoop.realtime.records.Counters;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.records.JobReport;
import org.apache.hadoop.realtime.records.JobState;
import org.apache.hadoop.realtime.records.TaskReport;
import org.apache.hadoop.realtime.records.TaskState;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class DragonJob {

  private static final Log LOG = LogFactory.getLog(DragonJob.class);

  private static final long MAX_JOBSTATE_AGE = 500;

  public static final String USED_GENERIC_PARSER = 
      "dragon.client.genericoptionsparser.used";
  /**
   * The UserGroupInformation object that has a reference to the current user
   */
  protected UserGroupInformation ugi;

  private Configuration conf;
  
  private DragonJobService jobService;
  
  private JobId jobId;

  protected final Credentials credentials;
  
  private JobState state = JobState.NEW;

  DragonJob(final DragonConfiguration conf) throws IOException {
    this.conf = conf;
    this.jobService = null;
    this.credentials = new Credentials();
    try {
      this.ugi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates a new {@link DragonJob}
   * 
   * @return the {@link DragonJob}
   * @throws IOException
   */
  public static DragonJob getInstance() throws IOException {
    DragonConfiguration newConf = new DragonConfiguration();
    return new DragonJob(newConf);
  }

  /**
   * Creates a new {@link DragonJob} with a given {@link Configuration}.
   * 
   * The <code>GraphJob</code> makes a copy of the <code>Configuration</code> so
   * that any necessary internal modifications do not reflect on the incoming
   * parameter.
   * 
   * @param conf the configuration
   * @return the {@link DragonJob} , with no connection to a cluster yet.
   * @throws IOException
   */
  public static DragonJob getInstance(final Configuration conf) throws IOException {
    DragonConfiguration newConf = new DragonConfiguration(conf);
    return new DragonJob(newConf);
  }

  private synchronized void connect() throws IOException, InterruptedException,
      ClassNotFoundException {
    if (jobService == null) {
      jobService = ugi.doAs(new PrivilegedExceptionAction<DragonJobService>() {
        public DragonJobService run() throws IOException, InterruptedException,
            ClassNotFoundException {
          DragonJobServiceFactory serviceFactory = new DragonJobServiceFactory();
          try {
            return serviceFactory.create(conf);
          } catch (Exception e) {
            throw new IOException(e);
          }
        }
      });
    }
  }
  
  /**
   * Submit the job to the cluster and return immediately.
   * 
   * @throws IOException
   */
  public void submit() throws IOException, InterruptedException,
      ClassNotFoundException {
    ensureState(JobState.NEW);
    connect();

    // If user hasn't define job.jar,
    // retrieve the path of jar file from the call stack
    if (conf.get(DragonJobConfig.JOB_JAR) == null) {
      StackTraceElement[] stack = Thread.currentThread().getStackTrace();
      String className = null;
      for (int i = 1; (className = stack[i++].getClassName()) 
          != DragonJob.class.getName(); i++);
      String jar = findContainingJar(Class.forName(className));
      conf.set(DragonJobConfig.JOB_JAR, jar);
    }

    JobReport report = ugi.doAs(new PrivilegedExceptionAction<JobReport>() {
      public JobReport run() throws IOException, InterruptedException,
          ClassNotFoundException {
        return jobService.submitJob(DragonJob.this);
      }
    });
    state = report.getJobState();
    LOG.info("The url to track the job: " + report.getTrackingUrl());
  }

  private void ensureState(JobState state) throws IllegalStateException {
    if (state != this.state) {
      throw new IllegalStateException("Job in state "+ this.state + 
                                      " instead of " + state);
    }

    if ((state == JobState.RUNNING || state == JobState.INITED)
        && jobService == null) {
      throw new IllegalStateException("Job in state " + this.state
          + ", but it isn't attached to any job tracker!");
    }
  }

  void setJobId(JobId jobId) {
    this.jobId = jobId;
  }

  public JobId getID() {
    return jobId;
  }

  public void setName(String name) {
    conf.set(DragonJobConfig.JOB_NAME, name);
  }

  public String getName() {
    return null;
  }

  public Credentials getCredentials() {
    return credentials;
  }

  public Configuration getConfiguration() {
    return conf;
  }

  public String getUser() {
    return ugi.getUserName();
  }

  public String getQueueName() {
    return null;
  }
  
  /**
   * Monitor a job and print status in real-time as progress is made and tasks
   * fail.
   * 
   * @return true if the job succeeded
   * @throws IOException if communication to the job service fails
   */
  public boolean monitorAndPrintJob() throws IOException, InterruptedException {
    JobId jobId = getID();
    LOG.info("Running job: " + jobId);
    List<TaskReport> reports = null;
    int taskNums = 0;
    int toBeScheduledTasks = 0;
    while (true) {
      toBeScheduledTasks =0;
      Thread.sleep(MAX_JOBSTATE_AGE);
      if (jobId == null) {
        jobId = getID();
        continue;
      }
      LOG.info(jobId + " " + state);
      reports = getTaskReports(jobId);
      taskNums = reports.size();
      for(TaskReport report:reports){
        if(report.getTaskState()!=TaskState.RUNNING){
          toBeScheduledTasks++;
        }
      }
      LOG.info("Your job has "+taskNums+" tasks, "+toBeScheduledTasks+" of them havn't been scheduled.");
      if(toBeScheduledTasks==0){
        LOG.info("All task of "+ jobId +" is scheduled. Job starts running.");
        break;
      }
    }
    Map<String, CounterGroup> groups = null ;
    while(groups == null){
      groups= getCounters(jobId).getAllCounterGroups();
    }
    LOG.info(groups);   
    return true;
  }

  /**
   * Get events indicating completion (success/failure) of component tasks.
   * 
   * @param startFrom index to start fetching events from
   * @param numEvents number of events to fetch
   * @return an array of {@link TaskCompletionEvent}s
   * @throws IOException
   */
  public JobReport getJobReport(final JobId jobId) throws IOException,
      InterruptedException {
    return ugi.doAs(new PrivilegedExceptionAction<JobReport>() {
      @Override
      public JobReport run() throws IOException, InterruptedException {
        return jobService.getJobReport(jobId);
      }
    });
  }

  public Counters getCounters(final JobId jobId) throws IOException,
      InterruptedException {
    return ugi.doAs(new PrivilegedExceptionAction<Counters>() {
      @Override
      public Counters run() throws IOException, InterruptedException {
        return jobService.getCounters(jobId);
      }
    });
}
  public List<TaskReport> getTaskReports(final JobId jobId) throws IOException,
      InterruptedException {
    return ugi.doAs(new PrivilegedExceptionAction<List<TaskReport>>() {
      @Override
      public List<TaskReport> run() throws IOException, InterruptedException {
        return jobService.getTaskReports(jobId);
      }
    });
  }

  /**
   * Kill the running job.  Blocks until all job tasks have been
   * killed as well.  If the job is no longer running, it simply returns.
   * 
   * @throws IOException
   */
  public void kill() throws IOException, InterruptedException {
    ensureState(JobState.RUNNING);
    jobService.killJob(getID());
  }
  
  public void setJar(String jar) {
    ensureState(JobState.NEW);
    conf.set(DragonJobConfig.JOB_JAR, jar);
  }
  
  public void setJarByClass(Class<?> clazz) {
    ensureState(JobState.NEW);
    String jar = findContainingJar(clazz);
    conf.set(DragonJobConfig.JOB_JAR, jar);
  }

  /**
   * Find a jar that contains a class of the same name, if any. It will return a
   * jar file, even if that is not the first thing on the class path that has a
   * class with the same name.
   * 
   * @param my_class the class to find.
   * @return a jar file that contains the class
   */
  private static String findContainingJar(Class<?> my_class) {
    ClassLoader loader = my_class.getClassLoader();
    String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
    try {
      for(Enumeration itr = loader.getResources(class_file);
          itr.hasMoreElements();) {
        URL url = (URL) itr.nextElement();
        if ("jar".equals(url.getProtocol())) {
          String toReturn = url.getPath();
          if (toReturn.startsWith("file:")) {
            toReturn = toReturn.substring("file:".length());
          }
          // URLDecoder is a misnamed class, since it actually decodes
          // x-www-form-urlencoded MIME type rather than actual
          // URL encoding (which the file path has). Therefore it would
          // decode +s to ' 's which is incorrect (spaces are actually
          // either unencoded or encoded as "%20"). Replace +s first, so
          // that they are kept sacred during the decoding process.
          toReturn = toReturn.replaceAll("\\+", "%2B");
          toReturn = URLDecoder.decode(toReturn, "UTF-8");
          return toReturn.replaceAll("!.*$", "");
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return null;
  }

  public JobState getState() {
    ensureState(JobState.RUNNING);
    try {
      JobReport report = getJobReport(jobId);
      state = report.getJobState();
    } catch (Exception e) {
      LOG.warn("Got an exception wheng getting state of a job", e);
    }
    return null;
  }
  
  /**
   * Set the {@link Mapper} for the job.
   * @param cls the <code>Mapper</code> to use
   * @throws IllegalStateException if the job is submitted
   */
  public void setMapperClass(Class<? extends Mapper> clazz)
      throws IllegalStateException {
    ensureState(JobState.NEW);
    conf.setClass(DragonJobConfig.JOB_MAP_CLASS, clazz, Mapper.class);
  }

  /**
   * Set the {@link Reducer} for the job.
   * @param cls the <code>Reducer</code> to use
   * @throws IllegalStateException if the job is submitted
   */
  public void setReducerClass(Class<? extends Reducer> clazz)
      throws IllegalStateException {
    ensureState(JobState.NEW);
    conf.setClass(DragonJobConfig.JOB_REDUCE_CLASS, clazz, Reducer.class);
  }
}
