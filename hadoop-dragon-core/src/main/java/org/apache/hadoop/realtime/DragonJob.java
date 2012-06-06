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
import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.realtime.conf.DragonConfiguration;
import org.apache.hadoop.realtime.job.Job;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.records.JobReport;
import org.apache.hadoop.realtime.records.JobState;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * A {@link DragonJob} is the basic computing unit of Dragon. It allows the 
 * user to configure the job, submit it, control its execution, and query 
 * the state. The set methods only work until the job is submitted, 
 * otherwises they will throw an IllegalStateException. </p>
 *
 * <p>
 * The key component of a {@link DragonJob} is a {@link DragonJobGraph},
 * which is formed by a collection of {@link DragonVertex}s and directed 
 * {@link DragonEdge}s.
 * Normally user creates the application, describes various facets of the job 
 * , sets {@link DragonJobGraph} via {@link DragonJob} , and then 
 * submits the job and monitor its progress.
 * </p>
 * 
 * <p>Here is an example on how to submit a {@link DragonJob}:</p>
 * <p><blockquote><pre>
 *   Configuration conf = getConf();
 *   conf.setInt(INT_PROPERTY, 1);
 *   conf.set(STRING_PROPERTY, "VALUE");
 *   conf.set(DragonJobConfig.PROPERTY, "GRAPHJOB_VALUE");
 *   DragonJob job = DragonJob.getInstance(conf);
 *   job.setJobName("First Graph Job");
 *   
 *   DragonVertex source = new DragonVertex.Builder("source")
 *                                         .producer(EventProducer.class)
 *                                         .processor(EventProcessor.class)
 *                                         .tasks(10)
 *                                         .build();
 *   DragonVertex m1 = new DragonVertex.Builder("intermediate1")
 *                                     .processor(EventProcessor.class)
 *                                     .addFile("file.txt")
 *                                     .addFile("dict.dat")
 *                                     .addArchive("archive.zip")
 *                                     .tasks(10)
 *                                     .build();
 *   DragonVertex m2 = new DragonVertex.Builder("intermediate2")
 *                                     .processor(EventProcessor.class)
 *                                     .addFile("aux")
 *                                     .tasks(10)
 *                                     .build();
 *   DragonVertex dest = new DragonVertex.Builder("dest")
 *                                       .processor(EventProcessor.class)
 *                                       .tasks(10)
 *                                       .build();
 *   DragonJobGraph g = new DragonJobGraph();
 *   // check if the graph is cyclic when adding edge
 *   g.addEdge(source, m1).parition(HashPartitioner.class);
 *   g.addEdge(source, m2).parition(HashPartitioner.class);
 *   g.addEdge(m1, dest).parition(CustomPartitioner.class);
 *   g.addEdge(m2, dest).parition(CustomPartitioner.class);
 *   job.setJobGraph(g);
 *   // check all source vertex hold event producers when submitting
 *   job.submit();
 * </pre></blockquote></p>
 * 
 * @see DirectedAcyclicGraph
 * @see DragonVertex
 * @see DragonEdge
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class DragonJob implements Job {

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

  private DragonJobGraph jobGraph;

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

  @Override
  public JobId getID() {
    return jobId;
  }

  public void setJobName(String name) {
    conf.set(DragonJobConfig.JOB_NAME, name);
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public Credentials getCredentials() {
    return credentials;
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public String getUser() {
    return ugi.getUserName();
  }

  @Override
  public String getQueueName() {
    return null;
  }
  
  public void setJobGraph(DragonJobGraph jobGraph) {
    this.jobGraph =  jobGraph;
  }
  
  DragonJobGraph getJobGraph() {
    return jobGraph;
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
    JobReport report = null;
    while (state != JobState.SUCCEEDED) {
      Thread.sleep(MAX_JOBSTATE_AGE);
      if (jobId == null) {
        jobId = getID();
        continue;
      }
      LOG.info(jobId + " " + state);
      report = getJobReport(jobId);
      state = report.getJobState();
    }
    LOG.info(report.getDiagnostics());
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

  /**
   * Kill the running job.  Blocks until all job tasks have been
   * killed as well.  If the job is no longer running, it simply returns.
   * 
   * @throws IOException
   */
  public void killJob() throws IOException, InterruptedException {
    ensureState(JobState.RUNNING);
    jobService.killJob(getID());
  }
}
