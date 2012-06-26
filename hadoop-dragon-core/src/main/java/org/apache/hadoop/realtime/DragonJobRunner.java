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
/**
 * 
 */
package org.apache.hadoop.realtime;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.realtime.client.ClientCache;
import org.apache.hadoop.realtime.records.Counters;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.records.JobReport;
import org.apache.hadoop.realtime.records.JobState;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.TaskReport;
import org.apache.hadoop.realtime.security.TokenCache;
import org.apache.hadoop.realtime.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.realtime.serialize.HessianSerializer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * {@link DragonJobRunner} is a yarn based {@link DragonJobService}
 * implementation.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class DragonJobRunner implements DragonJobService {
  private static final Log LOG = LogFactory.getLog(DragonJobRunner.class);

  private ResourceMgrDelegate resMgrDelegate;
  private ClientCache clientCache;
  private Configuration conf;

  /**
   * For {@link DragonJobServiceFactory}
   */
  DragonJobRunner() {
  }

  /**
   * Dragon runner incapsulates the client interface of yarn
   * 
   * @param conf the configuration object for the client
   */
  public DragonJobRunner(Configuration conf) {
    setConf(conf);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    try {
      this.resMgrDelegate = new ResourceMgrDelegate(new YarnConfiguration(conf));
      this.clientCache = new ClientCache(conf, resMgrDelegate);
    } catch (Exception e) {
      throw new RuntimeException("Error in instantiating YarnClient", e);
    }
  }

  @Override
  public JobId getNewJobId() throws IOException, InterruptedException {
    return resMgrDelegate.getNewJobID();
  }

  /**
   * Method for submitting jobs to the system.
   * 
   * <p>
   * The job submission process involves:
   * 
   * <ol>
   * <li>
   * Checking the first vertex has specified input data path or not</li>
   * <li>
   * Computing the {@link InputSplit}s for the job.</li>
   * <li>
   * Fetching a new application id from resource manager</li>
   * <li>
   * Optionally getting secret keys and tokens from namenodes and store them
   * into TokenCache</li>
   * <li>
   * Coping local files/archives into remote filesystem (e.g. hdfs)</li>
   * <li>
   * Writing job config files (job.xml, job.desc) to submit dir</li>
   * <li>
   * Optionally getting the security token of the jobSubmitDir and store in
   * Credentials</li>
   * <li>
   * Creating submission context for the job and launch context for application
   * master
   * <li>Submitting the application to resource manager and optionally
   * monitoring it's status.</li>
   * </p>
   * 
   * @param job the job to submit
   * @throws InterruptedException
   * @throws IOException
   */
  @Override
  public JobReport submitJob(DragonJob job) throws IOException,
      InterruptedException {
    Configuration conf = job.getConfiguration();

    if (!(conf.getBoolean(DragonJob.USED_GENERIC_PARSER, false))) {
      LOG.warn("Use GenericOptionsParser for parsing the arguments. "
          + "Applications should implement Tool for the same.");
    }

    // configure submit host
    configureSubmitHost(conf);
    // fetch a new job id from resource manager
    JobId jobId = getNewJobId();
    job.setJobId(jobId);

    Path jobStagingArea = JobSubmissionFiles.getStagingDir(this, conf);
    Path submitJobDir = new Path(jobStagingArea, jobId.toString());
    FileSystem submitFs = submitJobDir.getFileSystem(conf);
    submitJobDir = submitFs.makeQualified(submitJobDir);
    submitJobDir = new Path(submitJobDir.toUri().getPath());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Default FileSystem: " + submitFs.getUri());
    }

    short replication =
        (short) conf.getInt(DragonJobConfig.JOB_SUBMIT_FILE_REPLICATION, 10);

    try {
      // conf.set("hadoop.http.filter.initializers",
      // "org.apache.hadoop.yarn.server.webproxy.amfilter.AmFilterInitializer");
      conf.set(DragonJobConfig.JOB_SUBMIT_DIR, submitJobDir.toString());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Configuring job " + jobId + " with " + submitJobDir
            + " as the submit dir");
      }
      // get delegation token for the dir
      TokenCache.obtainTokensForNamenodes(job.getCredentials(),
          new Path[] { submitJobDir }, conf);

      populateTokenCache(conf, job.getCredentials());

      copyFilesFromLocal(job.getJobGraph(), submitFs, submitJobDir, replication);
      copyJobJar(job, conf, submitJobDir, submitFs, replication);

      // write "queue admins of the queue to which job is being submitted"
      // to job file.
      configureQueueAdmins(conf);

      // removing jobtoken referrals before copying the jobconf to HDFS
      // as the tasks don't need this setting, actually they may break
      // because of it if present as the referral will point to a
      // different job.
      TokenCache.cleanUpTokenReferral(conf);

      // Write job file to submit dir
      writeConf(conf, submitFs, submitJobDir);

      // Write the serialized job description dag to submit dir
      writeJobDescription(job.getJobGraph(), submitFs, submitJobDir);

      //
      // Now, actually submit the job (using the submit name)
      //
      printTokens(jobId, job.getCredentials());
    } finally {
      // if (status == null) {
      // LOG.info("Cleaning up the staging area " + submitJobDir);
      // if (jtFs != null && submitJobDir != null)
      // jtFs.delete(submitJobDir, true);
      //
      // }
    }

    // Get the security token of the jobSubmitDir and store in Credentials
    Path applicationTokensFile =
        new Path(submitJobDir, DragonJobConfig.APPLICATION_TOKENS_FILE);
    Credentials ts = job.getCredentials();
    try {
      ts.writeTokenStorageFile(applicationTokensFile, conf);
    } catch (IOException e) {
      throw new YarnException(e);
    }
    // Construct necessary information to start the Dragon AM
    ApplicationSubmissionContext appContext =
        createApplicationSubmissionContext(submitFs, submitJobDir, ts);
    // Submit to ResourceManager
    ApplicationId applicationId = resMgrDelegate.submitApplication(appContext);
    ApplicationReport appMaster =
        resMgrDelegate.getApplicationReport(applicationId);
    String diagnostics =
        (appMaster == null ? "application report is null" : appMaster
            .getDiagnostics());
    if (appMaster == null
        || appMaster.getYarnApplicationState() == YarnApplicationState.FAILED
        || appMaster.getYarnApplicationState() == YarnApplicationState.KILLED) {
      throw new IOException("Failed to run job : " + diagnostics);
    }
    return clientCache.getClient(jobId).getJobReport(jobId);
  }

  private void copyJobJar(DragonJob job, Configuration conf, Path submitJobDir,
      FileSystem submitFs, short replication) throws IOException {
    String originalJar = conf.get(DragonJobConfig.JOB_JAR);
    if(originalJar != null) {
      if ("".equals(job.getName())){
        job.setName(new Path(originalJar).getName());
      }
      Path destJarPath = JobSubmissionFiles.getJobJar(submitJobDir);
      submitFs.copyFromLocalFile(false, true, new Path(originalJar), destJarPath);
      submitFs.setReplication(destJarPath, replication);
      conf.set(DragonJobConfig.JOB_JAR, destJarPath.toString());
    } else {
      LOG.warn("No job jar file set.  User classes may not be found. "+
      "See Job or Job#setJar(String).");
    }
  }
  
  private void configureQueueAdmins(Configuration conf) throws IOException {
    String queue =
        conf.get(DragonJobConfig.QUEUE_NAME,
            DragonJobConfig.DEFAULT_QUEUE_NAME);
    AccessControlList acl = getQueueAdmins(queue);
    conf.set(
        toFullPropertyName(queue, QueueACL.ADMINISTER_JOBS.getAclName()),
        acl.getAclString());
  }

  private void configureSubmitHost(Configuration conf)
      throws UnknownHostException {
    InetAddress ip = InetAddress.getLocalHost();
    if (ip != null) {
      conf.set(DragonJobConfig.JOB_SUBMITHOST, ip.getHostName());
      conf.set(DragonJobConfig.JOB_SUBMITHOSTADDR, ip.getHostAddress());
    }
  }

  @Override
  public String getSystemDir() throws IOException, InterruptedException {
    return resMgrDelegate.getSystemDir();
  }

  @Override
  public String getStagingAreaDir() throws IOException, InterruptedException {
    return resMgrDelegate.getStagingAreaDir();
  }

  ApplicationSubmissionContext createApplicationSubmissionContext(
      final FileSystem submitFs, final Path jobSubmitDir, final Credentials ts)
      throws IOException {
    ApplicationId applicationId = resMgrDelegate.getApplicationId();

    // Setup capability
    Resource capability = DragonApps.setupResources(conf);
    capability.setMemory(conf.getInt(DragonJobConfig.DRAGON_AM_VMEM_MB,
        DragonJobConfig.DEFAULT_DRAGON_AM_VMEM_MB));
    if(LOG.isDebugEnabled()) {
      LOG.debug("DragonAppMaster capability = " + capability);
    }

    // Setup LocalResources
    Map<String, LocalResource> localResources =
        new HashMap<String, LocalResource>();

    // Job configuration file
    Path jobConfPath = new Path(jobSubmitDir, DragonJobConfig.JOB_CONF_FILE);
    localResources.put(DragonJobConfig.JOB_CONF_FILE,
        DragonApps.createApplicationResource(submitFs, jobConfPath));
    
    // JobJar
    if (conf.get(DragonJobConfig.JOB_JAR) != null) {
      localResources.put(
          DragonJobConfig.JOB_JAR,
          DragonApps.createApplicationResource(submitFs,
              new Path(conf.get(DragonJobConfig.JOB_JAR))));
    } else {
      LOG.info("Job jar is not present. "
          + "Not adding any jar to the list of resources.");
    }

    // Setup security tokens
    ByteBuffer securityTokens = null;
    if (UserGroupInformation.isSecurityEnabled()) {
      DataOutputBuffer dob = new DataOutputBuffer();
      ts.writeTokenStorageToStream(dob);
      securityTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    }
    
    // Setup the command to run the AM
    List<String> vargs = setupAMCommand(conf);

    // Setup the CLASSPATH in environment
    Map<String, String> environment = new HashMap<String, String>();
    DragonApps.setClasspath(environment, conf);

    // Setup the ACLs
    Map<ApplicationAccessType, String> acls = DragonApps.setupACLs(conf);

    // Generate ContainerLaunchContext for AM container
    ContainerLaunchContext amContainer =
        BuilderUtils.newContainerLaunchContext(null, UserGroupInformation
            .getCurrentUser().getShortUserName(), capability, localResources,
            environment, vargs, null, securityTokens, acls);

    return DragonApps.newApplicationSubmissionContext(applicationId, conf,
        amContainer);
  }

  /**
   * Setup the commands to run the application master
   */
  private List<String> setupAMCommand(Configuration conf) {
    Vector<CharSequence> vargs = new Vector<CharSequence>(8);
    vargs.add(Environment.JAVA_HOME.$() + "/bin/java");

    //addLog4jSystemProperties(conf, vargs);

    vargs.add(conf.get(DragonJobConfig.DRAGON_AM_COMMAND_OPTS,
        DragonJobConfig.DEFAULT_DRAGON_AM_COMMAND_OPTS));

    vargs.add(DragonJobConfig.APPLICATION_MASTER_CLASS);
    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
        + Path.SEPARATOR + ApplicationConstants.STDOUT);
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
        + Path.SEPARATOR + ApplicationConstants.STDERR);

    // Final commmand
    StringBuilder mergedCommand = new StringBuilder();
    for (CharSequence str : vargs) {
      mergedCommand.append(str).append(" ");
    }
    List<String> vargsFinal = new ArrayList<String>(8);
    vargsFinal.add(mergedCommand.toString());
    return vargsFinal;
  }

  @Override
  public AccessControlList getQueueAdmins(String queueName) throws IOException {
    return new AccessControlList("*");
  }

  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException, InterruptedException {
    // The token is only used for serialization. So the type information
    // mismatch should be fine.
    return resMgrDelegate.getDelegationToken(renewer);
  }

  @Override
  public void killJob(JobId jobId) throws IOException, InterruptedException {
    /* check if the status is not running, if not send kill to RM */
    JobState state =
        clientCache.getClient(jobId).getJobReport(jobId).getJobState();
    if (state != JobState.RUNNING) {
      resMgrDelegate.killApplication(jobId.getAppId());
      return;
    }
    try {
      /* send a kill to the AM */
      clientCache.getClient(jobId).killJob(jobId);
      long currentTimeMillis = System.currentTimeMillis();
      long timeKillIssued = currentTimeMillis;
      while ((currentTimeMillis < timeKillIssued + 10000L)
          && (state != JobState.KILLED)) {
        try {
          Thread.sleep(1000L);
        } catch (InterruptedException ie) {
          /** interrupted, just break */
          break;
        }
        currentTimeMillis = System.currentTimeMillis();
        state = clientCache.getClient(jobId).getJobReport(jobId).getJobState();
      }
    } catch (IOException io) {
      LOG.info("Error when checking for application status", io);
    }
    if (state != JobState.KILLED) {
      resMgrDelegate.killApplication(jobId.getAppId());
    }
  }

  @Override
  public boolean killTask(TaskAttemptId taskId, boolean shouldFail)
      throws IOException, InterruptedException {
    clientCache.getClient(taskId.getTaskId().getJobId()).killTask(taskId,
        shouldFail);
    return false;
  }

  @Override
  public List<TaskReport> getTaskReports(JobId jobId) throws IOException,
      InterruptedException {
    return clientCache.getClient(jobId).getTaskReports(jobId);
  }

  @Override
  public JobReport getJobReport(JobId jobId) throws YarnRemoteException {
    return clientCache.getClient(jobId).getJobReport(jobId);
  }

  private void writeConf(final Configuration conf, final FileSystem submitFs,
      final Path submitJobDir) throws IOException {
    // Write job file to job service provider's fs
    Path jobFile = JobSubmissionFiles.getJobConfPath(submitJobDir);
    FSDataOutputStream out =
        FileSystem.create(submitFs, jobFile, new FsPermission(
            JobSubmissionFiles.JOB_FILE_PERMISSION));
    try {
      conf.writeXml(out);
    } finally {
      out.close();
    }
  }

  private void writeJobDescription(final DragonJobGraph graph,
      final FileSystem submitFs, final Path submitJobDir) throws IOException {
    // Write job description to job service provider's fs
    Path descFile = JobSubmissionFiles.getJobDescriptionFile(submitJobDir);
    FSDataOutputStream out =
        FileSystem.create(submitFs, descFile, new FsPermission(
            JobSubmissionFiles.JOB_FILE_PERMISSION));
    try {
      HessianSerializer<DragonJobGraph> serializer =
          new HessianSerializer<DragonJobGraph>();
      serializer.serialize(out, graph);
    } finally {
      out.close();
    }
  }

  // get secret keys and tokens and store them into TokenCache
  @SuppressWarnings("unchecked")
  private void populateTokenCache(Configuration conf, Credentials credentials)
      throws IOException {
    readTokensFromFiles(conf, credentials);
    // add the delegation tokens from configuration
    String[] nameNodes = conf.getStrings(DragonJobConfig.JOB_NAMENODES);
    LOG.debug("adding the following namenodes' delegation tokens:"
        + Arrays.toString(nameNodes));
    if (nameNodes != null) {
      Path[] ps = new Path[nameNodes.length];
      for (int i = 0; i < nameNodes.length; i++) {
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
      Credentials binary =
          Credentials.readTokenStorageFile(new Path("file:///"
              + binaryTokenFilename), conf);
      credentials.addAll(binary);
    }
    // add secret keys coming from a json file
    String tokensFileName =
        conf.get(DragonJobConfig.DRAGON_JOB_CREDENTIALS_JSON);
    if (tokensFileName != null) {
      LOG.info("loading user's secret keys from " + tokensFileName);
      String localFileName = new Path(tokensFileName).toUri().getPath();

      boolean json_error = false;
      try {
        // read JSON
        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> nm =
            mapper.readValue(new File(localFileName), Map.class);

        for (Map.Entry<String, String> ent : nm.entrySet()) {
          credentials.addSecretKey(new Text(ent.getKey()), ent.getValue()
              .getBytes());
        }
      } catch (JsonMappingException e) {
        json_error = true;
      } catch (JsonParseException e) {
        json_error = true;
      }
      if (json_error)
        LOG.warn("couldn't parse Token Cache JSON file with user secret keys");
    }
  }

  private void copyFilesFromLocal(final DragonJobGraph jobGraph,
                                  final FileSystem submitFs,
                                  final Path submitJobDir,
                                  final short replication) 
      throws IOException {
    if (submitFs.exists(submitJobDir)) {
      throw new IOException("Not submitting job. Job directory " + submitJobDir
          + " already exists!! This is unexpected.Please check what's there in"
          + " that directory");
    }
    FsPermission sysPerms =
        new FsPermission(JobSubmissionFiles.JOB_DIR_PERMISSION);
    FileSystem.mkdirs(submitFs, submitJobDir, sysPerms);
    // add all the command line files/ jars and archive
    // first copy them to dragon job service provider's
    // filesystem
    for (DragonVertex vertex : jobGraph.vertexSet()) {
      List<String> files = vertex.getFiles();
      if (files.size() > 0) {
        Path filesDir = JobSubmissionFiles.getJobDistCacheFiles(submitJobDir);
        FileSystem.mkdirs(submitFs, filesDir, sysPerms);
        for (String file : files) {
          Path newPath = new Path(filesDir, file);
          submitFs.copyFromLocalFile(false, true, new Path(file), newPath);
          submitFs.setReplication(newPath, replication);
        }
      }

      List<String> archives = vertex.getArchives();
      if (archives.size() > 0) {
        Path archivesDir =
            JobSubmissionFiles.getJobDistCacheArchives(submitJobDir);
        FileSystem.mkdirs(submitFs, archivesDir, sysPerms);
        for (String archive : archives) {
          Path newPath = new Path(archivesDir, archive);
          submitFs.copyFromLocalFile(false, true, new Path(archive),
              newPath);
          submitFs.setReplication(newPath, replication);
        }
      }
    }
  }

  private void printTokens(JobId jobId, Credentials credentials)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Printing tokens for job: " + jobId);
      for (Token<?> token : credentials.getAllTokens()) {
        if (token.getKind().toString().equals("HDFS_DELEGATION_TOKEN")) {
          LOG.debug("Submitting with "
              + DelegationTokenIdentifier.stringifyToken(token));
        }
      }
    }
  }

  // this method is for internal use only
  private static final String toFullPropertyName(String queue, String property) {
    return "dragon.queue." + queue + "." + property;
  }

  @Override
  public Counters getCounters(JobId jobId) throws YarnRemoteException {
    return clientCache.getClient(jobId).getCounters(jobId);
  }

}
