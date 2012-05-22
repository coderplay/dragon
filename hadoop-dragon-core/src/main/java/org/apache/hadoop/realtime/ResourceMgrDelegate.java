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
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.DelegationToken;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;

// TODO: This should be part of something like yarn-client.
public class ResourceMgrDelegate {
	private static final Log LOG = LogFactory.getLog(ResourceMgrDelegate.class);

	private final String rmAddress;
	private YarnConfiguration conf;
	ClientRMProtocol applicationsManager;
	private ApplicationId applicationId;
	private final RecordFactory recordFactory = RecordFactoryProvider
	    .getRecordFactory(null);

	/**
	 * Delegate responsible for communicating with the Resource Manager's
	 * {@link ClientRMProtocol}.
	 * 
	 * @param conf
	 *          the configuration object.
	 */
	public ResourceMgrDelegate(YarnConfiguration conf) {
		this.conf = conf;
		YarnRPC rpc = YarnRPC.create(this.conf);
		InetSocketAddress rmAddress = NetUtils.createSocketAddr(this.conf.get(
		    YarnConfiguration.RM_ADDRESS, YarnConfiguration.DEFAULT_RM_ADDRESS),
		    YarnConfiguration.DEFAULT_RM_PORT, YarnConfiguration.RM_ADDRESS);
		this.rmAddress = rmAddress.toString();
		LOG.debug("Connecting to ResourceManager at " + rmAddress);
		applicationsManager = (ClientRMProtocol) rpc.getProxy(
		    ClientRMProtocol.class, rmAddress, this.conf);
		LOG.debug("Connected to ResourceManager at " + rmAddress);
	}

	/**
	 * Used for injecting applicationsManager, mostly for testing.
	 * 
	 * @param conf
	 *          the configuration object
	 * @param applicationsManager
	 *          the handle to talk the resource managers {@link ClientRMProtocol}.
	 */
	public ResourceMgrDelegate(YarnConfiguration conf,
	    ClientRMProtocol applicationsManager) {
		this.conf = conf;
		this.applicationsManager = applicationsManager;
		this.rmAddress = applicationsManager.toString();
	}

	public YarnClusterMetrics getClusterMetrics() throws IOException,
	    InterruptedException {
		GetClusterMetricsRequest request = recordFactory
		    .newRecordInstance(GetClusterMetricsRequest.class);
		GetClusterMetricsResponse response = applicationsManager
		    .getClusterMetrics(request);
		YarnClusterMetrics metrics = response.getClusterMetrics();
		return metrics;
	}

	@SuppressWarnings("rawtypes")
	public Token getDelegationToken(Text renewer) throws IOException,
	    InterruptedException {
		/* get the token from RM */
		org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest rmDTRequest = recordFactory
		    .newRecordInstance(org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest.class);
		rmDTRequest.setRenewer(renewer.toString());
		org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse response = applicationsManager
		    .getDelegationToken(rmDTRequest);
		DelegationToken yarnToken = response.getRMDelegationToken();
		return new Token<RMDelegationTokenIdentifier>(yarnToken.getIdentifier()
		    .array(), yarnToken.getPassword().array(),
		    new Text(yarnToken.getKind()), new Text(yarnToken.getService()));
	}

	public String getFilesystemName() throws IOException, InterruptedException {
		return FileSystem.get(conf).getUri().toString();
	}

	public JobId getNewJobID() throws IOException, InterruptedException {
		GetNewApplicationRequest request = recordFactory
		    .newRecordInstance(GetNewApplicationRequest.class);
		applicationId = applicationsManager.getNewApplication(request)
		    .getApplicationId();
		return new JobId(applicationId);

	}

	public String getSystemDir() throws IOException, InterruptedException {
		Path sysDir = new Path(DragonJobConfig.JOB_SUBMIT_DIR);
		return sysDir.toString();
	}

	public ApplicationId
	    submitApplication(ApplicationSubmissionContext appContext)
	        throws IOException {
		appContext.setApplicationId(applicationId);
		SubmitApplicationRequest request = recordFactory
		    .newRecordInstance(SubmitApplicationRequest.class);
		request.setApplicationSubmissionContext(appContext);
		applicationsManager.submitApplication(request);
		LOG.info("Submitted application " + applicationId + " to ResourceManager"
		    + " at " + rmAddress);
		return applicationId;
	}

	public void killApplication(ApplicationId applicationId) throws IOException {
		KillApplicationRequest request = recordFactory
		    .newRecordInstance(KillApplicationRequest.class);
		request.setApplicationId(applicationId);
		applicationsManager.forceKillApplication(request);
		LOG.info("Killing application " + applicationId);
	}

	public ApplicationReport getApplicationReport(ApplicationId appId)
	    throws YarnRemoteException {
		GetApplicationReportRequest request = recordFactory
		    .newRecordInstance(GetApplicationReportRequest.class);
		request.setApplicationId(appId);
		GetApplicationReportResponse response = applicationsManager
		    .getApplicationReport(request);
		ApplicationReport applicationReport = response.getApplicationReport();
		return applicationReport;
	}

	public ApplicationId getApplicationId() {
		return applicationId;
	}
  private static final String STAGING_CONSTANT = ".staging";
	public String getStagingAreaDir() throws IOException, InterruptedException {
		String user = UserGroupInformation.getCurrentUser().getShortUserName();
    Path path= new Path(
        conf.get(DragonJobConfig.DRAGON_AM_STAGING_DIR) + 
        Path.SEPARATOR + user + Path.SEPARATOR + STAGING_CONSTANT);
		LOG.debug("getStagingAreaDir: dir=" + path);
		return path.toString();
	}
}
