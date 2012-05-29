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

package org.apache.hadoop.realtime.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.realtime.server.DragonApplicationMaster;
import org.apache.hadoop.realtime.util.DSConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public class DragonClient {

  private static final Log LOG = LogFactory.getLog(DragonClient.class);

  private ClientRMProtocol applicationsManager;
  private Configuration conf;
  private YarnRPC rpc;

  private int amMemory = 10;
  private String appName = "";
  private String appMasterJar = "";
  private String appMasterMainClass = "";
  private String childJar = "";
  private String childClass = "";
  private String childArgs = "";
  private Map<String, String> childEnv = new HashMap<String, String>();

  private int childClassPriority = 0;
  private int containerMemory = 10;
  private int numContainers = 1;

  private String log4jPropFile = "";

  private int amPriority = 0;
  private String amQueue = "";
  private String amUser = "";

  public static void main(String[] args) {
    boolean result = false;
    try {
      DragonClient client = new DragonClient();
      LOG.info("Initializing Client");
      boolean doRun = client.init(args);
      if (!doRun) {
        System.exit(0);
      }
      result = client.run();
    } catch (Throwable t) {
      LOG.fatal("Error running CLient", t);
      System.exit(1);
    }
    if (result) {
      LOG.info("Application completed successfully");
      System.exit(0);
    }
    LOG.error("Application failed!");
    System.exit(2);
  }

  public DragonClient() throws Exception {
    conf = new Configuration();
    rpc = YarnRPC.create(conf);
  }

  public boolean init(String[] args) throws ParseException {
    Options opts = new Options();
    opts.addOption("appname", true,
        "Application Name. Default value - DistributedShell");
    opts.addOption("priority", true, "Application Priority. Default 0");
    opts.addOption("queue", true,
        "RM Queue in which this application is to be submitted");
    opts.addOption("user", true, "User to run the application as");
    opts.addOption("master_memory", true,
        "Amount of memory in MB to be requested to run the application master");
    opts.addOption("jar", true, "Jar file containing the application master");
    opts.addOption("child_jar", true,
        "Location of the Child Class JAR to be executed");
    opts.addOption("class", true,
        "Main class to  be run for the Application Master.");
    opts.addOption("child_class", true,
        "Java child class to be executed by the Container");
    opts.addOption("child_args", true, "Command line args for the child class");
    opts.addOption("child_env", true,
        "Environment for child class. Specified as env_key=env_val pairs");
    opts.addOption("child_class_priority", true,
        "Priority for the child class containers");
    opts.addOption("container_memory", true,
        "Amount of memory in MB to be requested to run the shell command");
    opts.addOption("num_containers", true,
        "No. of containers on which the shell command needs to be executed");
    opts.addOption("log_properties", true, "log4j.properties file");
    opts.addOption("debug", false, "Dump out debug information");
    opts.addOption("help", false, "Print usage");
    CommandLine cliParser = new GnuParser().parse(opts, args);
    if (args.length == 0) {
      printUsage(opts);
      System.err.println("No args specified for client to initialize");
      return false;
    }

    if (cliParser.hasOption("help")) {
      printUsage(opts);
      return false;
    }
    appName = cliParser.getOptionValue("appname", "DistributedStream");
    amPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
    amQueue = cliParser.getOptionValue("queue", "");
    amUser = cliParser.getOptionValue("user", "");
    amMemory =
        Integer.parseInt(cliParser.getOptionValue("master_memory", "10"));
    if (amMemory < 0) {
      System.err
          .println("Invalid memory specified for application master, exiting."
              + " Specified memory=" + amMemory);
      return false;
    }

    if (!cliParser.hasOption("jar")) {
      System.err.println("No jar file specified for application master");
      return false;
    }
    appMasterJar = cliParser.getOptionValue("jar");
    appMasterMainClass =
        cliParser.getOptionValue("class",
            DragonApplicationMaster.class.getName());
    childJar = cliParser.getOptionValue("child_jar", appMasterJar);
    if (!cliParser.hasOption("child_class")) {
      System.err
          .println("No child_class specified to be executed by container");
      return false;
    }
    childClass = cliParser.getOptionValue("child_class");
    if (cliParser.hasOption("child_args")) {
      childArgs = cliParser.getOptionValue("child_args");
    }
    if (cliParser.hasOption("child_env")) {
      String envs[] = cliParser.getOptionValues("child_env");
      for (String env : envs) {
        env = env.trim();
        int index = env.indexOf('=');
        if (index == -1) {
          childEnv.put(env, "");
          continue;
        }
        String key = env.substring(0, index);
        String val = "";
        if (index < (env.length() - 1)) {
          val = env.substring(index + 1);
        }
        childEnv.put(key, val);
      }
    }
    childClassPriority =
        Integer.parseInt(cliParser.getOptionValue("shell_cmd_priority", "0"));

    containerMemory =
        Integer.parseInt(cliParser.getOptionValue("container_memory", "10"));
    numContainers =
        Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));

    if (containerMemory < 0 || numContainers < 1) {
      System.err
          .println("Invalid no. of containers or container memory specified, exiting."
              + " Specified containerMemory="
              + containerMemory
              + ", numContainer=" + numContainers);
      return false;
    }
    log4jPropFile = cliParser.getOptionValue("log_properties", "");

    return true;
  }

  /**
   * Helper function to print out usage
   * 
   * @param opts Parsed command line options
   */
  private void printUsage(Options opts) {
    new HelpFormatter().printHelp("Client", opts);
  }

  public boolean run() throws IOException {
    LOG.info("Starting Client");

    // Connect to ResourceManager
    connectToASM();
    assert (applicationsManager != null);

    // Use ClientRMProtocol handle to general cluster information
    GetClusterMetricsRequest clusterMetricsReq =
        Records.newRecord(GetClusterMetricsRequest.class);
    GetClusterMetricsResponse clusterMetricsResp =
        applicationsManager.getClusterMetrics(clusterMetricsReq);
    LOG.info("Got Cluster metric info from ASM" + ", numNodeManagers="
        + clusterMetricsResp.getClusterMetrics().getNumNodeManagers());

    GetClusterNodesRequest clusterNodesReq =
        Records.newRecord(GetClusterNodesRequest.class);
    GetClusterNodesResponse clusterNodesResp =
        applicationsManager.getClusterNodes(clusterNodesReq);
    LOG.info("Got Cluster node info from ASM");
    for (NodeReport node : clusterNodesResp.getNodeReports()) {
      LOG.info("Got node report from ASM for" + ", nodeId=" + node.getNodeId()
          + ", nodeAddress" + node.getHttpAddress() + ", nodeRackName"
          + node.getRackName() + ", nodeNumContainers"
          + node.getNumContainers() + ", nodeHealthStatus"
          + node.getNodeHealthStatus());
    }

    GetQueueInfoRequest queueInfoReq =
        Records.newRecord(GetQueueInfoRequest.class);
    GetQueueInfoResponse queueInfoResp =
        applicationsManager.getQueueInfo(queueInfoReq);
    QueueInfo queueInfo = queueInfoResp.getQueueInfo();
    LOG.info("Queue info" + ", queueName=" + queueInfo.getQueueName()
        + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity()
        + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
        + ", queueApplicationCount=" + queueInfo.getApplications().size()
        + ", queueChildQueueCount=" + queueInfo.getChildQueues().size());

    GetQueueUserAclsInfoRequest queueUserAclsReq =
        Records.newRecord(GetQueueUserAclsInfoRequest.class);
    GetQueueUserAclsInfoResponse queueUserAclsResp =
        applicationsManager.getQueueUserAcls(queueUserAclsReq);
    List<QueueUserACLInfo> listAclInfo =
        queueUserAclsResp.getUserAclsInfoList();
    for (QueueUserACLInfo aclInfo : listAclInfo) {
      for (QueueACL userAcl : aclInfo.getUserAcls()) {
        LOG.info("User ACL Info for Queue" + ", queueName="
            + aclInfo.getQueueName() + ", userAcl=" + userAcl.name());
      }
    }

    
    // Request for an application id from resource manager
    GetNewApplicationResponse newApp = getApplication();
    ApplicationId appId = newApp.getApplicationId();

    // 获取当前ResourceManager中的内存信息
    int minMem = newApp.getMinimumResourceCapability().getMemory();
    int maxMem = newApp.getMaximumResourceCapability().getMemory();
    LOG.info("Min mem capabililty of resources in this cluster " + minMem);
    LOG.info("Max mem capabililty of resources in this cluster " + maxMem);
    if (amMemory < minMem) {
      LOG.info("AM memory specified below min threshold of cluster. Using min value."
          + ", specified=" + amMemory + ", min=" + minMem);
      amMemory = minMem;
    } else if (amMemory > maxMem) {
      LOG.info("AM memory specified above max threshold of cluster. Using max value."
          + ", specified=" + amMemory + ", max=" + maxMem);
      amMemory = maxMem;
    }

    /**
     * 初始化Application
     */
    ApplicationSubmissionContext appContext =
        Records.newRecord(ApplicationSubmissionContext.class);
    appContext.setApplicationId(appId);
    appContext.setApplicationName(appName);

    Map<String, String> env = new HashMap<String, String>();

 
     // set up the jar for AM
    ContainerLaunchContext amContainer =
        Records.newRecord(ContainerLaunchContext.class);
    Map<String, LocalResource> localResources =
        new HashMap<String, LocalResource>();
    FileSystem fs = FileSystem.get(conf);
    Path src = new Path(appMasterJar);
    String pathSuffix = appName + "/" + appId.getId() + "/AppMaster.jar";
    Path dst = new Path(fs.getHomeDirectory(), pathSuffix);
    fs.copyFromLocalFile(false, true, src, dst);
    FileStatus destStatus = fs.getFileStatus(dst);
    LocalResource amJarRsrc = Records.newRecord(LocalResource.class);
    amJarRsrc.setType(LocalResourceType.FILE);
    amJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
    amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(dst));
    amJarRsrc.setTimestamp(destStatus.getModificationTime());
    amJarRsrc.setSize(destStatus.getLen());
    localResources.put("AppMaster.jar", amJarRsrc);

    //set up the jar for child class
    if (childJar.equals(appMasterJar) || childJar == appMasterJar) {
      env.put(DSConstants.DISTRIBUTED_CHILDCLASS_LOCATION, dst.toUri()
          .toString());
      env.put(DSConstants.DISTRIBUTED_CHILDCLASS_TIMESTAMP,
          Long.toString(destStatus.getModificationTime()));
      env.put(DSConstants.DISTRIBUTED_CHILDCLASS_LEN,
          Long.toString(destStatus.getLen()));
    } else {
      Path childSrc = new Path(childJar);
      String childPathSuffix = appName + "/" + appId.getId() + "/Child.jar";
      Path childDst = new Path(fs.getHomeDirectory(), childPathSuffix);
      fs.copyFromLocalFile(false, true, childSrc, childDst);
      env.put(DSConstants.DISTRIBUTED_CHILDCLASS_LOCATION, childDst.toUri()
          .toString());
      FileStatus childFileStatus = fs.getFileStatus(childDst);
      env.put(DSConstants.DISTRIBUTED_CHILDCLASS_TIMESTAMP,
          Long.toString(childFileStatus.getModificationTime()));
      env.put(DSConstants.DISTRIBUTED_CHILDCLASS_LEN,
          Long.toString(childFileStatus.getLen()));
    }

    //configure log4j
    if (!log4jPropFile.isEmpty()) {
      Path log4jSrc = new Path(log4jPropFile);
      Path log4jDst = new Path(fs.getHomeDirectory(), "log4j.props");
      fs.copyFromLocalFile(false, true, log4jSrc, log4jDst);
      FileStatus log4jFileStatus = fs.getFileStatus(log4jDst);
      LocalResource log4jRsrc = Records.newRecord(LocalResource.class);
      log4jRsrc.setType(LocalResourceType.FILE);
      log4jRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
      log4jRsrc.setResource(ConverterUtils.getYarnUrlFromURI(log4jDst.toUri()));
      log4jRsrc.setTimestamp(log4jFileStatus.getModificationTime());
      log4jRsrc.setSize(log4jFileStatus.getLen());
      localResources.put("log4j.properties", log4jRsrc);
    }

    amContainer.setLocalResources(localResources);

    // 
    LOG.info("Set the environment for the application master");

    StringBuilder classPathEnv = new StringBuilder("${CLASSPATH}:./*");
    for (String c : conf.get(YarnConfiguration.YARN_APPLICATION_CLASSPATH)
        .split(",")) {
      classPathEnv.append(':');
      classPathEnv.append(c.trim());
    }
    classPathEnv.append(":./log4j.properties");
    String testRuntimeClassPath = DragonClient.getTestRuntimeClasspath();
    classPathEnv.append(':');
    classPathEnv.append(testRuntimeClassPath);
    env.put("CLASSPATH", classPathEnv.toString());
    amContainer.setEnvironment(env);

    // Set the necessary command to execute the application master
    Vector<CharSequence> vargs = new Vector<CharSequence>(30);

    // Set java executable command
    LOG.info("Setting up app master command");
    vargs.add("${JAVA_HOME}" + "/bin/java");
    // Set Xmx based on am memory size
    vargs.add("-Xmx" + amMemory + "m");
    // Set class name
    vargs.add(appMasterMainClass);
    vargs.add("--container_memory " + String.valueOf(containerMemory));
    vargs.add("--num_containers " + String.valueOf(numContainers));
    vargs.add("--priority " + String.valueOf(childClassPriority));
    if (!childClass.isEmpty()) {
      vargs.add("--child_class " + childClass + "");
    }
    if (!childArgs.isEmpty()) {
      vargs.add("--child_args " + childArgs + "");
    }
    for (Map.Entry<String, String> entry : childEnv.entrySet()) {
      vargs.add("--child_env " + entry.getKey() + "=" + entry.getValue());
    }
    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
        + "/AppMaster.stdout");
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
        + "/AppMaster.stderr");
    StringBuilder command = new StringBuilder();
    for (CharSequence str : vargs) {
      command.append(str).append(" ");
    }

    LOG.info("Completed setting up app master command " + command.toString());
    List<String> commands = new ArrayList<String>();
    commands.add(command.toString());
    amContainer.setCommands(commands);

    // For launching an AM Container, setting user here is not needed
    // Set user in ApplicationSubmissionContext
    // amContainer.setUser(amUser);

    // Set up resource type requirements
    // For now, only memory is supported so we set memory requirements
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(amMemory);
    amContainer.setResource(capability);

    appContext.setAMContainerSpec(amContainer);

    // Set the priority for the application master
    Priority pri = Records.newRecord(Priority.class);
    // TODO - what is the range for priority? how to decide?
    pri.setPriority(amPriority);
    appContext.setPriority(pri);

    // Set the queue to which this application is to be submitted in the RM
    appContext.setQueue(amQueue);
    // Set the user submitting this application
    // TODO can it be empty?
    appContext.setUser(amUser);

    // Create the request to send to the applications manager
    SubmitApplicationRequest appRequest =
        Records.newRecord(SubmitApplicationRequest.class);
    appRequest.setApplicationSubmissionContext(appContext);

    // Submit the application to the applications manager
    // SubmitApplicationResponse submitResp =
    // applicationsManager.submitApplication(appRequest);
    // Ignore the response as either a valid response object is returned on
    // success
    // or an exception thrown to denote some form of a failure
    LOG.info("Submitting application to ASM");
    applicationsManager.submitApplication(appRequest);

    return monitorApplication(appId);

  }

  private void connectToASM() throws IOException {
    YarnConfiguration yarnConf = new YarnConfiguration(conf);
    InetSocketAddress rmAddress =
        NetUtils.createSocketAddr(yarnConf.get(YarnConfiguration.RM_ADDRESS,
            YarnConfiguration.DEFAULT_RM_ADDRESS));

    LOG.info("Connecting to ResourceManager at " + rmAddress);
    applicationsManager =
        ((ClientRMProtocol) rpc.getProxy(ClientRMProtocol.class, rmAddress,
            conf));
  }

  private GetNewApplicationResponse getApplication() throws YarnRemoteException {
    GetNewApplicationRequest request =
        Records.newRecord(GetNewApplicationRequest.class);
    GetNewApplicationResponse response =
        applicationsManager.getNewApplication(request);
    LOG.info("Got new application id=" + response.getApplicationId());
    return response;
  }

  private boolean monitorApplication(ApplicationId appId)
      throws YarnRemoteException {

    while (true) {

      // Check app status every 1 second.
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.debug("Thread sleep in monitoring loop interrupted");
      }
      
 /*     // Get application report for the appId we are interested in
      GetApplicationReportRequest reportRequest =
          Records.newRecord(GetApplicationReportRequest.class);
      reportRequest.setApplicationId(appId);
      GetApplicationReportResponse reportResponse =
          applicationsManager.getApplicationReport(reportRequest);
      ApplicationReport report = reportResponse.getApplicationReport();

      LOG.info("Got application report from ASM for" + ", appId="
          + appId.getId() + ", clientToken=" + report.getClientToken()
          + ", appDiagnostics=" + report.getDiagnostics() + ", appMasterHost="
          + report.getHost() + ", appQueue=" + report.getQueue()
          + ", appMasterRpcPort=" + report.getRpcPort() + ", appStartTime="
          + report.getStartTime() + ", yarnAppState="
          + report.getYarnApplicationState().toString()
          + ", distributedFinalState="
          + report.getFinalApplicationStatus().toString() + ", appTrackingUrl="
          + report.getTrackingUrl() + ", appUser=" + report.getUser());

      YarnApplicationState state = report.getYarnApplicationState();
      FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
      if (YarnApplicationState.FINISHED == state) {
        if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
          LOG.info("Application has completed successfully. Breaking monitoring loop");
          return true;
        } else {
          LOG.info("Application did finished unsuccessfully." + " YarnState="
              + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
              + ". Breaking monitoring loop");
          return false;
        }
      } else if (YarnApplicationState.KILLED == state
          || YarnApplicationState.FAILED == state) {
        LOG.info("Application did not finish." + " YarnState="
            + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
            + ". Breaking monitoring loop");
        return false;
      }*/

    }
  }

  private static String getTestRuntimeClasspath() {

    InputStream classpathFileStream = null;
    BufferedReader reader = null;
    String envClassPath = "";

    LOG.info("Trying to generate classpath for app master from current thread's classpath");
    try {

      // Create classpath from generated classpath
      // Check maven ppom.xml for generated classpath info
      // Works if compile time env is same as runtime. Mainly tests.
      ClassLoader thisClassLoader =
          Thread.currentThread().getContextClassLoader();
      String generatedClasspathFile = "yarn-apps-ds-generated-classpath";
      classpathFileStream =
          thisClassLoader.getResourceAsStream(generatedClasspathFile);
      if (classpathFileStream == null) {
        LOG.info("Could not classpath resource from class loader");
        return envClassPath;
      }
      LOG.info("Readable bytes from stream=" + classpathFileStream.available());
      reader = new BufferedReader(new InputStreamReader(classpathFileStream));
      String cp = reader.readLine();
      if (cp != null) {
        envClassPath += cp.trim() + ":";
      }
      // Put the file itself on classpath for tasks.
      envClassPath +=
          thisClassLoader.getResource(generatedClasspathFile).getFile();
    } catch (IOException e) {
      LOG.info("Could not find the necessary resource to generate class path for tests. Error="
          + e.getMessage());
    }

    try {
      if (classpathFileStream != null) {
        classpathFileStream.close();
      }
      if (reader != null) {
        reader.close();
      }
    } catch (IOException e) {
      LOG.info("Failed to close class path file stream or reader. Error="
          + e.getMessage());
    }
    return envClassPath;
  }
}
