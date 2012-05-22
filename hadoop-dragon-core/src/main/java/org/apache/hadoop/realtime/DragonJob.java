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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.realtime.job.Job;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * A {@link DragonJob} is the basic computing unit of Dragon. It allows the 
 * user to configure the job, submit it, control its execution, and query 
 * the state. The set methods only work until the job is submitted, 
 * otherwises they will throw an IllegalStateException. </p>
 *
 * <p>
 * Normally the user creates the application, describes various facets of the
 * job via {@link DragonJob} and then submits the job and monitor its progress.
 * </p>
 * 
 * <p>Here is an example on how to submit a Dragon {@link DragonJob}:</p>
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
 *                                     .cache("file.txt,dict.dat")
 *                                     .tasks(10)
 *                                     .build();
 *   DragonVertex m2 = new DragonVertex.Builder("intermediate2")
 *                                     .processor(EventProcessor.class)
 *                                     .tasks(10)
 *                                     .build();
 *   DragonVertex dest = new DragonVertex.Builder("source")
 *                                       .processor(EventProcessor.class)
 *                                       .tasks(10)
 *                                       .build();
 *   DirectedAcyclicGraph<DragonVertex, DragonEdge> g = 
 *       new DirectedAcyclicGraph<DragonVertex, DragonEdge>();
 *   // check if the graph is cyclic when adding edge
 *   g.addEdge(source, m1).parition(HashPartitioner.class);
 *   g.addEdge(source, m2).parition(HashPartitioner.class);
 *   g.addEdge(m1, dest).parition(CustomPartitioner.class);
 *   g.addEdge(m2, dest).parition(CustomPartitioner.class);
 *   job.setGraph(serializer.serialze(g));
 *   // check all source vertex hold event producers when submitting
 *   job.submit();
 * </pre></blockquote></p>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class DragonJob implements Job {

  /**
   * The UserGroupInformation object that has a reference to the current user
   */
  protected UserGroupInformation ugi;

  private Configuration conf;
  
  private Cluster cluster;

  DragonJob(final Configuration conf) throws IOException {
    this.conf = new Configuration(conf);
    this.cluster = null;
  }

  /**
   * Creates a new {@link DragonJob}
   * 
   * @return the {@link DragonJob}
   * @throws IOException
   */
  public static DragonJob getInstance() throws IOException {
    // create with a null Cluster
    return getInstance(new Configuration());
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
    // create with a null Cluster
    return new DragonJob(conf);
  }

  private synchronized void connect() throws IOException, InterruptedException,
      ClassNotFoundException {
    if (cluster == null) {
      cluster = ugi.doAs(new PrivilegedExceptionAction<Cluster>() {
        public Cluster run() throws IOException, InterruptedException,
            ClassNotFoundException {
          return new Cluster(getConfiguration());
        }
      });
    }
  }
  
  public JobSubmitter getJobSubmitter(FileSystem fs,
      DragonJobService submitClient) throws IOException {
    return new JobSubmitter(fs, submitClient);
  }
  
  /**
   * Submit the job to the cluster and return immediately.
   * 
   * @throws IOException
   */
  public void submit() throws IOException, InterruptedException,
      ClassNotFoundException {
    connect();
    final FileSystem fs = FileSystem.get(conf);
    final JobSubmitter submitter =
        getJobSubmitter(cluster.getFileSystem(), cluster.getClient());

    ugi.doAs(new PrivilegedExceptionAction<Boolean>() {
      public Boolean run() throws IOException, InterruptedException,
          ClassNotFoundException {
        return submitter.submitJobInternal(DragonJob.this, cluster);
      }
    });
  }

  @Override
  public JobId getJobId() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getJobName() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Credentials getCredentials() {
    return null;
  }

  @Override
  public Configuration getConfiguration() {
    return null;
  }

  @Override
  public String getUser() {
    return null;
  }
}