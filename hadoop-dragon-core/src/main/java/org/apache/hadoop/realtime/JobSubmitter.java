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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.realtime.dag.DirectedAcyclicGraph;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.security.TokenCache;
import org.apache.hadoop.realtime.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.realtime.serialize.HessianSerializer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class JobSubmitter {
  protected static final Log LOG = LogFactory.getLog(JobSubmitter.class);
  private FileSystem submitFs;
  private String submitHostName;
  private String submitHostAddress;
  
  DragonJobService client;
  
  JobSubmitter(FileSystem submitFs, DragonJobService client)
      throws IOException {
    this.submitFs = submitFs;
    this.client = client;
  }

  /**
   * Internal method for submitting jobs to the system.
   * 
   * <p>The job submission process involves:
   * <ol>
   *   <li>
   *   Checking the input and output specifications of the job.
   *   </li>
   *   <li>
   *   Serializing the job description file for the job.
   *   </li>
   *   <li>
   *   Setup the requisite accounting information for the 
   *   DistributedCache of the job, if necessary.
   *   </li>
   *   <li>
   *   Copying the job's jar and configuration to the dragon system
   *   directory on the distributed file-system. 
   *   </li>
   *   <li>
   *   Submitting the job to the {@link DragonJobService} and optionally
   *   monitoring it's status.
   *   </li>
   * </ol></p>
   * @param job the configuration to submit
   * @param cluster the handle to the Cluster
   * @throws ClassNotFoundException
   * @throws InterruptedException
   * @throws IOException
   */
  boolean submitJobInternal(DragonJob job, Cluster cluster) 
      throws ClassNotFoundException, InterruptedException, IOException {
    Path jobStagingArea = JobSubmissionFiles.getStagingDir(cluster, 
        job.getConfiguration());
    Configuration conf = job.getConfiguration();
    InetAddress ip = InetAddress.getLocalHost();
    if (ip != null) {
      submitHostAddress = ip.getHostAddress();
      submitHostName = ip.getHostName();
      conf.set(DragonJobConfig.JOB_SUBMITHOST,submitHostName);
      conf.set(DragonJobConfig.JOB_SUBMITHOSTADDR,submitHostAddress);
    }
    
    JobId jobId = client.getNewJobId();
    job.setJobId(jobId);

    Path submitJobDir = new Path(jobStagingArea, jobId.toString());

    try {
      conf.set("hadoop.http.filter.initializers", 
          "org.apache.hadoop.yarn.server.webproxy.amfilter.AmFilterInitializer");
      conf.set(DragonJobConfig.JOB_SUBMIT_DIR, submitJobDir.toString());
      if(LOG.isDebugEnabled()) {
        LOG.debug("Configuring job " + jobId + " with " + submitJobDir 
            + " as the submit dir");
      }
      // get delegation token for the dir
      TokenCache.obtainTokensForNamenodes(job.getCredentials(),
          new Path[] { submitJobDir }, conf);
      
      populateTokenCache(conf, job.getCredentials());

      copyAndConfigureFiles(job, submitJobDir);
      Path submitJobFile = JobSubmissionFiles.getJobConfPath(submitJobDir);
      Path submitJobDescFile = JobSubmissionFiles.getJobDescriptionFile(submitJobDir);

      // write "queue admins of the queue to which job is being submitted"
      // to job file.
      String queue = conf.get(DragonJobConfig.QUEUE_NAME,
          DragonJobConfig.DEFAULT_QUEUE_NAME);
      AccessControlList acl = client.getQueueAdmins(queue);
      conf.set(toFullPropertyName(queue,
          QueueACL.ADMINISTER_JOBS.getAclName()), acl.getAclString());

      // removing jobtoken referrals before copying the jobconf to HDFS
      // as the tasks don't need this setting, actually they may break
      // because of it if present as the referral will point to a
      // different job.
      TokenCache.cleanUpTokenReferral(conf);

      // Write job file to submit dir
      writeConf(conf, submitJobFile);
      
      // Write the serialized job description dag to submit dir
      writeJobDescription(job.getJobGraph(), submitJobDescFile);

      //
      // Now, actually submit the job (using the submit name)
      //
      printTokens(jobId, job.getCredentials());
      return client.submitJob(
          jobId, submitJobDir.toString(), job.getCredentials());
    } finally {
//      if (status == null) {
//        LOG.info("Cleaning up the staging area " + submitJobDir);
//        if (jtFs != null && submitJobDir != null)
//          jtFs.delete(submitJobDir, true);
//
//      }
    }
  }
  
  private void writeConf(Configuration conf, Path jobFile) throws IOException {
    // Write job file to job service provider's fs
    FSDataOutputStream out =
        FileSystem.create(submitFs, jobFile, new FsPermission(
            JobSubmissionFiles.JOB_FILE_PERMISSION));
    try {
      conf.writeXml(out);
    } finally {
      out.close();
    }
  }
  
  private void writeJobDescription(
      DirectedAcyclicGraph<DragonVertex, DragonEdge> graph, Path descFile)
      throws IOException {
    // Write job description to job service provider's fs
    FSDataOutputStream out =
        FileSystem.create(submitFs, descFile, new FsPermission(
            JobSubmissionFiles.JOB_FILE_PERMISSION));
    try {
      HessianSerializer<DirectedAcyclicGraph<DragonVertex, DragonEdge>> serializer =
          new HessianSerializer<DirectedAcyclicGraph<DragonVertex, DragonEdge>>();
      serializer.serialize(out, graph);
    } finally {
      out.close();
    }
  }

  //get secret keys and tokens and store them into TokenCache
  @SuppressWarnings("unchecked")
  private void populateTokenCache(Configuration conf, Credentials credentials) 
  throws IOException{
    readTokensFromFiles(conf, credentials);
    // add the delegation tokens from configuration
    String [] nameNodes = conf.getStrings(DragonJobConfig.JOB_NAMENODES);
    LOG.debug("adding the following namenodes' delegation tokens:" + 
        Arrays.toString(nameNodes));
    if(nameNodes != null) {
      Path [] ps = new Path[nameNodes.length];
      for(int i=0; i< nameNodes.length; i++) {
        ps[i] = new Path(nameNodes[i]);
      }
      TokenCache.obtainTokensForNamenodes(credentials, ps, conf);
    }
  }

  @SuppressWarnings("unchecked")
  private void readTokensFromFiles(Configuration conf, Credentials credentials)
  throws IOException {
    // add tokens and secrets coming from a token storage file
    String binaryTokenFilename =
      conf.get(DragonJobConfig.DRAGON_JOB_CREDENTIALS_BINARY);
    if (binaryTokenFilename != null) {
      Credentials binary = Credentials.readTokenStorageFile(
          new Path("file:///" + binaryTokenFilename), conf);
      credentials.addAll(binary);
    }
    // add secret keys coming from a json file
    String tokensFileName =
        conf.get(DragonJobConfig.DRAGON_JOB_CREDENTIALS_JSON);
    if(tokensFileName != null) {
      LOG.info("loading user's secret keys from " + tokensFileName);
      String localFileName = new Path(tokensFileName).toUri().getPath();

      boolean json_error = false;
      try {
        // read JSON
        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> nm = 
          mapper.readValue(new File(localFileName), Map.class);

        for(Map.Entry<String, String> ent: nm.entrySet()) {
          credentials.addSecretKey(new Text(ent.getKey()), ent.getValue()
              .getBytes());
        }
      } catch (JsonMappingException e) {
        json_error = true;
      } catch (JsonParseException e) {
        json_error = true;
      }
      if(json_error)
        LOG.warn("couldn't parse Token Cache JSON file with user secret keys");
    }
  }

  
  private void copyLocalFiles(DragonJob job, Path submitJobDir,
      short replication) throws IOException {
    //
    // Figure out what fs the job service provider is using. Copy the
    // job to it, under a temporary name. This allows DFS to work,
    // and under the local fs also provides UNIX-like object loading
    // semantics. (that is, if the job file is deleted right after
    // submission, we can still run the submission to completion)
    //

    // Create a number of filenames in the JobTracker's fs namespace
    LOG.debug("default FileSystem: " + submitFs.getUri());
    if (submitFs.exists(submitJobDir)) {
      throw new IOException("Not submitting job. Job directory " + submitJobDir
          + " already exists!! This is unexpected.Please check what's there in"
          + " that directory");
    }
    submitJobDir = submitFs.makeQualified(submitJobDir);
    submitJobDir = new Path(submitJobDir.toUri().getPath());
    FsPermission sysPerms =
        new FsPermission(JobSubmissionFiles.JOB_DIR_PERMISSION);
    FileSystem.mkdirs(submitFs, submitJobDir, sysPerms);
    // add all the command line files/ jars and archive
    // first copy them to dragon job service provider's
    // filesystem
    DirectedAcyclicGraph<DragonVertex, DragonEdge> jobGraph = job.getJobGraph();
    for (DragonVertex vertex : jobGraph.vertexSet()) {
      List<String> files = vertex.getFiles();
      if (files.size() > 0) {
        Path filesDir = JobSubmissionFiles.getJobDistCacheFiles(submitJobDir);
        FileSystem.mkdirs(submitFs, filesDir, sysPerms);
        for (String file : files) {
          submitFs.copyFromLocalFile(false, true, new Path(file), filesDir);
        }
      }

      List<String> archives = vertex.getArchives();
      if (archives.size() > 0) {
        Path archivesDir =
            JobSubmissionFiles.getJobDistCacheArchives(submitJobDir);
        FileSystem.mkdirs(submitFs, archivesDir, sysPerms);
        for (String archive : archives) {
          submitFs.copyFromLocalFile(false, true, new Path(archive),
              archivesDir);
        }
      }
    }
  }

  /**
   * configure the jobconf of the user with the command line options of
   * -libjars, -files, -archives.
   * 
   * @param conf
   * @throws IOException
   */
  private void copyAndConfigureFiles(DragonJob job, Path jobSubmitDir)
      throws IOException {
    Configuration conf = job.getConfiguration();
    if (!(conf.getBoolean(DragonJob.USED_GENERIC_PARSER, false))) {
      LOG.warn("Use GenericOptionsParser for parsing the arguments. "
          + "Applications should implement Tool for the same.");
    }
    short replication =
        (short) conf.getInt(DragonJobConfig.JOB_SUBMIT_FILE_REPLICATION, 10);
    copyLocalFiles(job, jobSubmitDir, replication);
//    // Set the working directory
//    if (job.getWorkingDirectory() == null) {
//      job.setWorkingDirectory(submitFs.getWorkingDirectory());
//    }
  }

  private void printTokens(JobId jobId,
      Credentials credentials) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Printing tokens for job: " + jobId);
      for(Token<?> token: credentials.getAllTokens()) {
        if (token.getKind().toString().equals("HDFS_DELEGATION_TOKEN")) {
          LOG.debug("Submitting with " +
              DelegationTokenIdentifier.stringifyToken(token));
        }
      }
    }
  }

  // this method is for internal use only
  private static final String toFullPropertyName(
    String queue,
    String property) {
    return "dragon.queue." + queue + "." + property;
  }

}