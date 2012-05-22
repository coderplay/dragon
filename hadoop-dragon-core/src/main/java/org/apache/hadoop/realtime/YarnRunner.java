/**
 * 
 */
package org.apache.hadoop.realtime;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.realtime.protocol.records.GetJobReportRequest;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class YarnRunner implements DragonJobService {
	private static final Log LOG = LogFactory.getLog(YarnRunner.class);

	private final RecordFactory recordFactory = RecordFactoryProvider
	    .getRecordFactory(null);
	private ResourceMgrDelegate resMgrDelegate;
	private Configuration conf;
	private final FileContext defaultFileContext;

	/**
	 * Yarn runner incapsulates the client interface of yarn
	 * 
	 * @param conf
	 *          the configuration object for the client
	 */
	public YarnRunner(Configuration conf) {
		this(conf, new ResourceMgrDelegate(new YarnConfiguration(conf)));
	}

	/**
	 * Similar to {@link #YARNRunner(Configuration)} but allowing injecting
	 * {@link ResourceMgrDelegate}. Enables mocking and testing.
	 * 
	 * @param conf
	 *          the configuration object for the client
	 * @param resMgrDelegate
	 *          the resourcemanager client handle.
	 */
	public YarnRunner(Configuration conf, ResourceMgrDelegate resMgrDelegate) {
		this.conf = conf;
		try {
			this.resMgrDelegate = resMgrDelegate;
			this.defaultFileContext = FileContext.getFileContext(this.conf);
		} catch (UnsupportedFileSystemException ufe) {
			throw new RuntimeException("Error in instantiating YarnClient", ufe);
		}
	}

	@Override
	public JobId getNewJobId() throws IOException, InterruptedException {
		return resMgrDelegate.getNewJobID();
	}

	@Override
	public boolean submitJob(JobId jobId, String jobSubmitDir, Credentials ts)
	    throws IOException, InterruptedException {
		Path applicationTokensFile = new Path(jobSubmitDir,
		    DragonJobConfig.APPLICATION_TOKENS_FILE);
		try {
			ts.writeTokenStorageFile(applicationTokensFile, conf);
		} catch (IOException e) {
			throw new YarnException(e);
		}
		// Construct necessary information to start the MR AM
		ApplicationSubmissionContext appContext = createApplicationSubmissionContext(
		    conf, jobSubmitDir, ts);

		// Submit to ResourceManager
		ApplicationId applicationId = resMgrDelegate.submitApplication(appContext);

		ApplicationReport appMaster = resMgrDelegate
		    .getApplicationReport(applicationId);
		String diagnostics = (appMaster == null ? "application report is null"
		    : appMaster.getDiagnostics());
		if (appMaster == null
		    || appMaster.getYarnApplicationState() == YarnApplicationState.FAILED
		    || appMaster.getYarnApplicationState() == YarnApplicationState.KILLED) {
			throw new IOException("Failed to run job : " + diagnostics);
		}

		return true;
	}

	@Override
	public String getSystemDir() throws IOException, InterruptedException {
		return resMgrDelegate.getSystemDir();
	}

	@Override
	public String getStagingAreaDir() throws IOException, InterruptedException {
		return resMgrDelegate.getStagingAreaDir();
	}

	public ApplicationSubmissionContext createApplicationSubmissionContext(
	    Configuration jobConf, String jobSubmitDir, Credentials ts)
	    throws IOException {
		ApplicationId applicationId = resMgrDelegate.getApplicationId();

		// Setup resource requirements
		Resource capability = recordFactory.newRecordInstance(Resource.class);
		capability.setMemory(conf.getInt(DragonJobConfig.DRAGON_AM_VMEM_MB,
		    DragonJobConfig.DEFAULT_DRAGON_AM_VMEM_MB));
		LOG.debug("AppMaster capability = " + capability);

		// Setup LocalResources
		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

		Path jobConfPath = new Path(jobSubmitDir, DragonJobConfig.JOB_CONF_FILE);

		URL yarnUrlForJobSubmitDir = ConverterUtils
		    .getYarnUrlFromPath(defaultFileContext.getDefaultFileSystem()
		        .resolvePath(
		            defaultFileContext.makeQualified(new Path(jobSubmitDir))));
		LOG.debug("Creating setup context, jobSubmitDir url is "
		    + yarnUrlForJobSubmitDir);

		localResources.put(DragonJobConfig.JOB_CONF_FILE,
		    createApplicationResource(defaultFileContext, jobConfPath));
		if (jobConf.get(DragonJobConfig.JAR) != null) {
			localResources.put(
			    DragonJobConfig.JOB_JAR,
			    createApplicationResource(defaultFileContext, new Path(jobSubmitDir,
			        DragonJobConfig.JOB_JAR)));
		} else {
			// Job jar may be null. For e.g, for pipes, the job jar is the hadoop
			// mapreduce jar itself which is already on the classpath.
			LOG.info("Job jar is not present. "
			    + "Not adding any jar to the list of resources.");
		}

		// TODO gross hack
		for (String s : new String[] { DragonJobConfig.JOB_METAINFO,
		    DragonJobConfig.APPLICATION_TOKENS_FILE }) {
			localResources.put(
			    DragonJobConfig.JOB_SUBMIT_DIR + "/" + s,
			    createApplicationResource(defaultFileContext, new Path(jobSubmitDir,
			        s)));
		}

		// Setup security tokens
		ByteBuffer securityTokens = null;
		if (UserGroupInformation.isSecurityEnabled()) {
			DataOutputBuffer dob = new DataOutputBuffer();
			ts.writeTokenStorageToStream(dob);
			securityTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
		}

		// Setup the command to run the AM
		List<String> vargs = new ArrayList<String>(8);
		vargs.add(Environment.JAVA_HOME.$() + "/bin/java");

		// TODO: why do we use 'conf' some places and 'jobConf' others?
		long logSize = conf.getLong(DragonJobConfig.TASK_USERLOG_LIMIT, 0) * 1024;
		String logLevel = jobConf.get(DragonJobConfig.DRAGON_AM_LOG_LEVEL,
		    DragonJobConfig.DEFAULT_DRAGON_AM_LOG_LEVEL);
		DragonApps.addLog4jSystemProperties(logLevel, logSize, vargs);

		vargs.add(conf.get(DragonJobConfig.DRAGON_AM_COMMAND_OPTS,
		    DragonJobConfig.DEFAULT_DRAGON_AM_COMMAND_OPTS));

		vargs.add(DragonJobConfig.APPLICATION_MASTER_CLASS);
		vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
		    + Path.SEPARATOR + ApplicationConstants.STDOUT);
		vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
		    + Path.SEPARATOR + ApplicationConstants.STDERR);

		Vector<String> vargsFinal = new Vector<String>(8);
		// Final commmand
		StringBuilder mergedCommand = new StringBuilder();
		for (CharSequence str : vargs) {
			mergedCommand.append(str).append(" ");
		}
		vargsFinal.add(mergedCommand.toString());

		LOG.debug("Command to launch container for ApplicationMaster is : "
		    + mergedCommand);

		// Setup the CLASSPATH in environment
		// i.e. add { Hadoop jars, job jar, CWD } to classpath.
		Map<String, String> environment = new HashMap<String, String>();
		DragonApps.setClasspath(environment, conf);

		// Parse distributed cache
		DragonApps.setupDistributedCache(jobConf, localResources);

		Map<ApplicationAccessType, String> acls = new HashMap<ApplicationAccessType, String>(
		    2);
		acls.put(ApplicationAccessType.VIEW_APP, jobConf.get(
		    DragonJobConfig.JOB_ACL_VIEW_JOB,
		    DragonJobConfig.DEFAULT_JOB_ACL_VIEW_JOB));
		acls.put(ApplicationAccessType.MODIFY_APP, jobConf.get(
		    DragonJobConfig.JOB_ACL_MODIFY_JOB,
		    DragonJobConfig.DEFAULT_JOB_ACL_MODIFY_JOB));

		// Setup ContainerLaunchContext for AM container
		ContainerLaunchContext amContainer = BuilderUtils
		    .newContainerLaunchContext(null, UserGroupInformation.getCurrentUser()
		        .getShortUserName(), capability, localResources, environment,
		        vargsFinal, null, securityTokens, acls);

		// Set up the ApplicationSubmissionContext
		ApplicationSubmissionContext appContext = recordFactory
		    .newRecordInstance(ApplicationSubmissionContext.class);
		appContext.setApplicationId(applicationId); // ApplicationId
		appContext.setUser( // User name
		    UserGroupInformation.getCurrentUser().getShortUserName());
		appContext.setQueue( // Queue name
		    jobConf.get(DragonJobConfig.QUEUE_NAME,
		        YarnConfiguration.DEFAULT_QUEUE_NAME));
		appContext.setApplicationName( // Job name
		    jobConf.get(DragonJobConfig.JOB_NAME,
		        YarnConfiguration.DEFAULT_APPLICATION_NAME));
		appContext.setCancelTokensWhenComplete(conf.getBoolean(
		    DragonJobConfig.JOB_CANCEL_DELEGATION_TOKEN, true));
		appContext.setAMContainerSpec(amContainer); // AM Container

		return appContext;
	}

	private LocalResource createApplicationResource(FileContext fs, Path p)
	    throws IOException {
		LocalResource rsrc = recordFactory.newRecordInstance(LocalResource.class);
		FileStatus rsrcStat = fs.getFileStatus(p);
		rsrc.setResource(ConverterUtils.getYarnUrlFromPath(fs
		    .getDefaultFileSystem().resolvePath(rsrcStat.getPath())));
		rsrc.setSize(rsrcStat.getLen());
		rsrc.setTimestamp(rsrcStat.getModificationTime());
		rsrc.setType(LocalResourceType.FILE);
		rsrc.setVisibility(LocalResourceVisibility.APPLICATION);
		return rsrc;
	}

}
