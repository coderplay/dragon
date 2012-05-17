package org.apache.hadoop.realtime.server;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.realtime.util.DSConstants;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.exceptions.impl.pb.YarnRemoteExceptionPBImpl;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.api.records.ContainerState;

/**
 * An ApplicationMaster for executing shell commands on a set of launched
 * containers using the YARN framework.
 * 
 * <p>
 * This class is meant to act as an example on how to write yarn-based
 * application masters.
 * </p>
 * 
 * <p>
 * The ApplicationMaster is started on a container by the
 * <code>ResourceManager</code>'s launcher. The first thing that the
 * <code>ApplicationMaster</code> needs to do is to connect and register itself
 * with the <code>ResourceManager</code>. The registration sets up information
 * within the <code>ResourceManager</code> regarding what host:port the
 * ApplicationMaster is listening on to provide any form of functionality to a
 * client as well as a tracking url that a client can use to keep track of
 * status/job history if needed.
 * </p>
 * 
 * <p>
 * The <code>ApplicationMaster</code> needs to send a heartbeat to the
 * <code>ResourceManager</code> at regular intervals to inform the
 * <code>ResourceManager</code> that it is up and alive. The
 * {@link AMRMProtocol#allocate} to the <code>ResourceManager</code> from the
 * <code>ApplicationMaster</code> acts as a heartbeat.
 * 
 * <p>
 * For the actual handling of the job, the <code>ApplicationMaster</code> has to
 * request the <code>ResourceManager</code> via {@link AllocateRequest} for the
 * required no. of containers using {@link ResourceRequest} with the necessary
 * resource specifications such as node location, computational
 * (memory/disk/cpu) resource requirements. The <code>ResourceManager</code>
 * responds with an {@link AllocateResponse} that informs the
 * <code>ApplicationMaster</code> of the set of newly allocated containers,
 * completed containers as well as current state of available resources.
 * </p>
 * 
 * <p>
 * For each allocated container, the <code>ApplicationMaster</code> can then set
 * up the necessary launch context via {@link ContainerLaunchContext} to specify
 * the allocated container id, local resources required by the executable, the
 * environment to be setup for the executable, commands to execute, etc. and
 * submit a {@link StartContainerRequest} to the {@link ContainerManager} to
 * launch and execute the defined commands on the given allocated container.
 * </p>
 * 
 * <p>
 * The <code>ApplicationMaster</code> can monitor the launched container by
 * either querying the <code>ResourceManager</code> using
 * {@link AMRMProtocol#allocate} to get updates on completed containers or via
 * the {@link ContainerManager} by querying for the status of the allocated
 * container's {@link ContainerId}.
 * 
 * <p>
 * After the job has been completed, the <code>ApplicationMaster</code> has to
 * send a {@link FinishApplicationMasterRequest} to the
 * <code>ResourceManager</code> to inform it that the
 * <code>ApplicationMaster</code> has been completed.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class DragonApplicationMaster {

	private static final Log LOG = LogFactory.getLog(DragonApplicationMaster.class);

	// Para used to connect ResourceManager
	private Configuration conf;
	private YarnRPC rpc;
	private AMRMProtocol resourceManager;

	// Para used to regist ResourceManager
	private ApplicationAttemptId appAttemptID;
	private String appMasterHostname = "";
	private int appMasterRpcPort = 1;
	private String appMasterTrackingUrl = "";

	// Unchanged Para About Containers
	private int containerMemory = 1024;
	private int numTotalContainers = 5;
	private int requestPriority = 1;

	// Dynamic changed Para About Containers
	private boolean appDone = false;
	private AtomicInteger rmRequestNum = new AtomicInteger();
	private AtomicInteger numCompletedContainers = new AtomicInteger();
	private AtomicInteger numAllocatedContainers = new AtomicInteger();
	private AtomicInteger numFailedContainers = new AtomicInteger();
	private CopyOnWriteArrayList<ContainerId> releasedContainers = new CopyOnWriteArrayList<ContainerId>();

	// Para about child JVM progress
	private String childClass;
	private String childArgs;
	private String childJarPath;
	private Long childJarPathTimestamp;
	private Long childJarPathLen;
	private final Map<String, String> childEnv = new HashMap<String, String>();

	private final List<Thread> launchThreads = new ArrayList<Thread>();

	/**
	 * Start AMStream
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		boolean result = false;
		try {
			DragonApplicationMaster appMaster = new DragonApplicationMaster();
			LOG.info("Initializing ApplicationMaster");
			boolean doRun = appMaster.init(args);
			if (!doRun) {
				System.exit(0);
			}
			result = appMaster.run();
		} catch (Throwable t) {
			LOG.fatal("Error running ApplicationMaster", t);
			System.exit(1);
		}
		if (result) {
			LOG.info("Application Master completed successfully. exiting");
			System.exit(0);
		} else {
			LOG.info("Application Master failed. exiting");
			System.exit(2);
		}
	}

	/**
	 * Parse command line options
	 * 
	 * @param args
	 *            Command line args
	 * @return Whether init successful and run should be invoked
	 * @throws ParseException
	 * @throws IOException
	 */
	public boolean init(String[] args) throws ParseException {
		Options opts = new Options();
		opts.addOption("child_class", true,
				"Java child class to be executed by the Container");
		opts.addOption("child_args", true,
				"Command line args for the child class");
		opts.addOption("child_env", true,
				"Environment for child class. Specified as env_key=env_val pairs");
		opts.addOption("priority", true,
				"Priority for the child class containers");
		opts.addOption("container_memory", true,
				"Amount of memory in MB to be requested to run the shell command");
		opts.addOption("num_containers", true,
				"No. of containers on which the shell command needs to be executed");
		opts.addOption("help", false, "Print usage");
		CommandLine cliParser = new GnuParser().parse(opts, args);

		if (args.length == 0) {
			printUsage(opts);
			throw new IllegalArgumentException(
					"No args specified for application master to initialize");
		}

		Map<String, String> envs = System.getenv();
		appAttemptID = Records.newRecord(ApplicationAttemptId.class);
		if (!envs.containsKey(ApplicationConstants.AM_CONTAINER_ID_ENV)) {
			if (cliParser.hasOption("app_attempt_id")) {
				String appIdStr = cliParser
						.getOptionValue("app_attempt_id", "");
				appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
			} else {
				throw new IllegalArgumentException(
						"Application Attempt Id not set in the environment");
			}
		} else {
			ContainerId containerId = ConverterUtils.toContainerId(envs
					.get(ApplicationConstants.AM_CONTAINER_ID_ENV));
			appAttemptID = containerId.getApplicationAttemptId();
		}
		LOG.info("Application master for app" + ", appId="
				+ appAttemptID.getApplicationId().getId()
				+ ", clustertimestamp="
				+ appAttemptID.getApplicationId().getClusterTimestamp()
				+ ", attemptId=" + appAttemptID.getAttemptId());

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
			String childEnvs[] = cliParser.getOptionValues("child_env");
			for (String env : childEnvs) {
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
		if (envs.containsKey(DSConstants.DISTRIBUTED_CHILDCLASS_LOCATION)) {
			childJarPath = envs
					.get(DSConstants.DISTRIBUTED_CHILDCLASS_LOCATION);
		}
		if (envs.containsKey(DSConstants.DISTRIBUTED_CHILDCLASS_TIMESTAMP)) {
			childJarPathTimestamp = Long.valueOf(envs
					.get(DSConstants.DISTRIBUTED_CHILDCLASS_TIMESTAMP));
		}
		if (envs.containsKey(DSConstants.DISTRIBUTED_CHILDCLASS_LEN)) {
			childJarPathLen = Long.valueOf(envs
					.get(DSConstants.DISTRIBUTED_CHILDCLASS_LEN));
		}
		containerMemory = Integer.parseInt(cliParser.getOptionValue(
				"container_memory", "10"));
		numTotalContainers = Integer.parseInt(cliParser.getOptionValue(
				"num_containers", "1"));
		requestPriority = Integer.parseInt(cliParser.getOptionValue("priority",
				"0"));

		return true;
	}

	/**
	 * Helper function to print usage
	 * 
	 * @param opts
	 *            Parsed command line options
	 */
	private void printUsage(Options opts) {
		new HelpFormatter().printHelp("AMStream", opts);
	}

	public DragonApplicationMaster() throws Exception {
		conf = new Configuration();
		rpc = YarnRPC.create(conf);
	}

	public boolean run() throws Exception {
		LOG.info("Starting ApplicationMaster");
		resourceManager = connectToRM();
		RegisterApplicationMasterResponse response = registerToRM();

		initMemory(response);

		/**
		 * 当Application未完成时，不断循玄1�7
		 */
		while (numCompletedContainers.get() < numTotalContainers && !appDone) {

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				LOG.info("Sleep interrupted " + e.getMessage());
			}

			/**
			 * 向ResourceManager发�1�7�申请Container
			 */
			int askCount = numTotalContainers - numAllocatedContainers.get();
			List<ResourceRequest> resourceReq = new ArrayList<ResourceRequest>();
			if (askCount > 0) {
				ResourceRequest containerAsk = setupContainerAskForRM(askCount);
				resourceReq.add(containerAsk);

				AMResponse amResp = sendContainerAskToRM(resourceReq);

				/**
				 * 给获取到的Container分配任务
				 */
				// Retrieve list of allocated containers from the response
				List<Container> allocatedContainers = amResp
						.getAllocatedContainers();
				LOG.info("Got response from RM for container ask, allocatedCnt="
						+ allocatedContainers.size());
				numAllocatedContainers.addAndGet(allocatedContainers.size());
				for (Container allocatedContainer : allocatedContainers) {
					Task runnableLaunchContainer = new Task(allocatedContainer);
					Thread launchThread = new Thread(runnableLaunchContainer);
					LOG.info("Launching JAVA_CHILD on a new container."
							+ ", containerId=" + allocatedContainer.getId()
							+ ", containerNode="
							+ allocatedContainer.getNodeId().getHost() + ":"
							+ allocatedContainer.getNodeId().getPort()
							+ ", containerNodeURI="
							+ allocatedContainer.getNodeHttpAddress()
							+ ", containerState"
							+ allocatedContainer.getState()
							+ ", containerResourceMemory"
							+ allocatedContainer.getResource().getMemory());
					launchThreads.add(launchThread);
					launchThread.start();
				}

			}
			/**
			 * 获取Container的完成状态并更新
			 */
			List<ContainerStatus> completedContainers = sendContainerAskToRM(
					resourceReq).getCompletedContainersStatuses();
			for (ContainerStatus containerStatus : completedContainers) {
				// increment counters for completed/failed containers
				int exitStatus = containerStatus.getExitStatus();
				if (0 != exitStatus) {
					numAllocatedContainers.decrementAndGet();
					numFailedContainers.incrementAndGet();
					LOG.error("run child progress failed");
				} else {
					numCompletedContainers.incrementAndGet();
					LOG.info("Container completed successfully."
							+ ", containerId="
							+ containerStatus.getContainerId());
				}

			}
			if (numCompletedContainers.get() == numTotalContainers) {
				appDone = true;
			}
		}
		for (Thread launchThread : launchThreads) {
			try {
				launchThread.join(10000);
			} catch (InterruptedException e) {
				LOG.info("Exception thrown in thread join: " + e.getMessage());
				e.printStackTrace();
			}
		}
		FinishApplicationMasterRequest finishReq = Records
				.newRecord(FinishApplicationMasterRequest.class);
		finishReq.setAppAttemptId(appAttemptID);
		boolean isSuccess = true;
		if (numFailedContainers.get() == 0) {
			finishReq
					.setFinishApplicationStatus(FinalApplicationStatus.SUCCEEDED);
		} else {
			finishReq.setFinishApplicationStatus(FinalApplicationStatus.FAILED);
			String diagnostics = "Diagnostics." + ", total="
					+ numTotalContainers + ", completed="
					+ numCompletedContainers.get() + ", allocated="
					+ numAllocatedContainers.get() + ", failed="
					+ numFailedContainers.get();
			finishReq.setDiagnostics(diagnostics);
			LOG.error(diagnostics);
			isSuccess = false;
		}
		resourceManager.finishApplicationMaster(finishReq);
		return isSuccess;

	}

	/**
	 * 初始化用于向ResourceManager发�1�7�申请的request，包含Container的个数和该request的优先级
	 * 
	 * @param numContainers
	 * @return
	 */
	private ResourceRequest setupContainerAskForRM(int numContainers) {
		ResourceRequest request = Records.newRecord(ResourceRequest.class);

		request.setHostName("*");
		request.setNumContainers(numContainers);

		Priority pri = Records.newRecord(Priority.class);
		pri.setPriority(requestPriority);
		request.setPriority(pri);

		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(containerMemory);
		request.setCapability(capability);

		return request;
	}

	/**
	 * 向ResourceManager发�1�7�申请，返回申请的结构1�7
	 * 申请中包含：这是第几次申请�1�7�Application的名称�1�7�申请Container的request、需要释放的Container
	 * 以及container分配的进庄1�7
	 * 
	 * @param requestedContainers
	 * @return
	 * @throws YarnRemoteException
	 */
	private AMResponse sendContainerAskToRM(
			List<ResourceRequest> requestedContainers)
			throws YarnRemoteException {
		AllocateRequest req = Records.newRecord(AllocateRequest.class);
		req.setResponseId(rmRequestNum.incrementAndGet());
		req.setApplicationAttemptId(appAttemptID);
		req.addAllAsks(requestedContainers);
		req.addAllReleases(releasedContainers);
		req.setProgress((float) numCompletedContainers.get()
				/ numTotalContainers);

		LOG.info("Sending request to RM for containers" + ", requestedSet="
				+ requestedContainers.size() + ", releasedSet="
				+ releasedContainers.size() + ", progress=" + req.getProgress());

		for (ResourceRequest rsrcReq : requestedContainers) {
			LOG.info("Requested container ask: " + rsrcReq.toString());
		}
		for (ContainerId id : releasedContainers) {
			LOG.info("Released container, id=" + id.getId());
		}

		AllocateResponse resp = resourceManager.allocate(req);
		return resp.getAMResponse();
	}

	private AMRMProtocol connectToRM() {
		YarnConfiguration yarnConf = new YarnConfiguration(conf);
		InetSocketAddress rmAddress = NetUtils.createSocketAddr(yarnConf.get(
				YarnConfiguration.RM_SCHEDULER_ADDRESS,
				YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS));
		LOG.info("Connecting to ResourceManager at " + rmAddress);
		return ((AMRMProtocol) rpc
				.getProxy(AMRMProtocol.class, rmAddress, conf));
	}

	private RegisterApplicationMasterResponse registerToRM()
			throws YarnRemoteException, UnknownHostException {
		InetAddress host = InetAddress.getLocalHost();
		
		RegisterApplicationMasterRequest appMasterRequest = Records
				.newRecord(RegisterApplicationMasterRequest.class);

		appMasterRequest.setApplicationAttemptId(appAttemptID);
		appMasterRequest.setHost(host.getCanonicalHostName());
		appMasterRequest.setRpcPort(9999);
		appMasterRequest.setTrackingUrl(host.getCanonicalHostName() + ":" + 9999);

		return resourceManager.registerApplicationMaster(appMasterRequest);
	}

	private class Task implements Runnable {

		private final Container container;
		private ContainerManager cm;

		public Task(Container container) {
			this.container = container;
		}

		/**
		 * Helper function to connect to CM
		 */
		private void connectToCM() {
			String cmIpPortStr = container.getNodeId().getHost() + ":"
					+ container.getNodeId().getPort();
			InetSocketAddress cmAddress = NetUtils
					.createSocketAddr(cmIpPortStr);
			this.cm = ((ContainerManager) rpc.getProxy(ContainerManager.class,
					cmAddress, conf));
		}

		@Override
		/**
		 * Connects to CM, sets up container launch context 
		 * for shell command and eventually dispatches the container 
		 * start request to the CM. 
		 */
		public void run() {
			connectToCM();
			LOG.info("Setting up container launch container for containerid="
					+ container.getId());
			ContainerLaunchContext ctx = Records
					.newRecord(ContainerLaunchContext.class);

			ctx.setContainerId(container.getId());
			ctx.setResource(container.getResource());
			try {
				ctx.setUser(UserGroupInformation.getCurrentUser()
						.getShortUserName());
			} catch (IOException e) {
				LOG.info("Getting current user info failed when trying to launch the container"
						+ e.getMessage());
			}
			ctx.setEnvironment(initClasspath());
			ctx.setLocalResources(initLocalResource());
			ctx.setCommands(initCommand());

			StartContainerRequest startReq = Records
					.newRecord(StartContainerRequest.class);
			startReq.setContainerLaunchContext(ctx);
			try {
				cm.startContainer(startReq);
			} catch (YarnRemoteException e) {
				LOG.error("Start container failed for :" + ", containerId="
						+ container.getId(), e);
			}
			monitorApplication(container.getId());

		}

		private boolean monitorApplication(ContainerId containerId) {
			GetContainerStatusRequest statusReq = Records
					.newRecord(GetContainerStatusRequest.class);
			statusReq.setContainerId(containerId);
			GetContainerStatusResponse statusResp = null;
			ContainerState state;
			try {
				while (true) {
					Thread.sleep(1000);
					statusResp = cm.getContainerStatus(statusReq);
					state = statusResp.getStatus().getState();
					if (ContainerState.COMPLETE == state) {
						LOG.info("Container did finished successfully."
								+ ", id=" + container.getId()
								+ " ContainerState=" + state.toString()
								+ ". Breaking monitoring loop");
						return true;
					} else if (ContainerState.RUNNING == state) {
						LOG.info("Container Status" + ", id="
								+ container.getId() + ", state="
								+ state.toString());
					}
				}

			} catch (YarnRemoteExceptionPBImpl e) {
				LOG.info("Container did finished successfully." + ", id="
						+ container.getId() + " ContainerState= COMPLETE");
				return true;
			} catch (InterruptedException e) {
				LOG.debug("Thread sleep in monitoring loop interrupted");
			} catch (YarnRemoteException e) {
				LOG.error(e.getClass() + e.getMessage());
			}
			return false;
		}

		private Map<String, String> initClasspath() {
			Map<String, String> env = new HashMap<String, String>();
			StringBuilder classPathEnv = new StringBuilder("${CLASSPATH}:./*");
			for (String c : conf.get(
					YarnConfiguration.YARN_APPLICATION_CLASSPATH).split(",")) {
				classPathEnv.append(':');
				classPathEnv.append(c.trim());
			}
			classPathEnv.append(":./log4j.properties");
			env.put("CLASSPATH", classPathEnv.toString());
			return env;
		}

		private Map<String, LocalResource> initLocalResource() {
			Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
			LocalResource clientJarRsrc = Records
					.newRecord(LocalResource.class);
			clientJarRsrc.setType(LocalResourceType.FILE);
			clientJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
			try {
				clientJarRsrc.setResource(ConverterUtils
						.getYarnUrlFromURI(new URI(childJarPath)));
			} catch (URISyntaxException e) {
				LOG.error("Error when trying to use shell script path specified in env"
						+ ", path=" + childJarPath);
				numAllocatedContainers.decrementAndGet();
				numFailedContainers.incrementAndGet();
			}
			clientJarRsrc.setTimestamp(childJarPathTimestamp);
			clientJarRsrc.setSize(childJarPathLen);
			localResources.put("Child.jar", clientJarRsrc);
			return localResources;
		}

		private List<String> initCommand() {
			StringBuilder command = new StringBuilder();
			Vector<CharSequence> vargs = new Vector<CharSequence>(30);
			vargs.add("${JAVA_HOME}" + "/bin/java");
			try {
				Class.forName(childClass);
			} catch (ClassNotFoundException e1) {
				LOG.error("Init StreamChild fail." + childClass);
			}
			vargs.add(childClass); // main of Child
			vargs.add(childArgs);
			vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
					+ "/Container.stdout");
			vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
					+ "/Container.stderr");
			for (CharSequence str : vargs) {
				command.append(str).append(" ");
			}
			List<String> commands = new ArrayList<String>();
			commands.add(command.toString());
			return commands;
		}
	}

	/**
	 * Set the memory of container
	 * 
	 * @param response
	 */
	private void initMemory(RegisterApplicationMasterResponse response) {
		int minMem = response.getMinimumResourceCapability().getMemory();
		int maxMem = response.getMaximumResourceCapability().getMemory();
		LOG.info("Min mem capabililty of resources in this cluster " + minMem);
		LOG.info("Max mem capabililty of resources in this cluster " + maxMem);
		if (containerMemory < minMem)
			containerMemory = minMem;
		else if (containerMemory > maxMem)
			containerMemory = maxMem;
	}

}
