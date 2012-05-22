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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Test YarnRunner and make sure the client side plugin works
 * fine
 */
public class TestYarnRunner extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestYarnRunner.class);
  private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  private YarnRunner yarnRunner;
  private ResourceMgrDelegate resourceMgrDelegate;
  private YarnConfiguration conf;
  private ApplicationId appId;
  private JobId jobId;
  private File testWorkDir =
      new File("target", TestYarnRunner.class.getName());
  private ApplicationSubmissionContext submissionContext;
  private static final String failString = "Rejected job";

  @Before
  public void setUp() throws Exception {
    resourceMgrDelegate = mock(ResourceMgrDelegate.class);
    conf = new YarnConfiguration();
    yarnRunner = new YarnRunner(conf, resourceMgrDelegate);
    yarnRunner = spy(yarnRunner);
    submissionContext = mock(ApplicationSubmissionContext.class);
    doAnswer(
        new Answer<ApplicationSubmissionContext>() {
          @Override
          public ApplicationSubmissionContext answer(InvocationOnMock invocation)
              throws Throwable {
            return submissionContext;
          }
        }
        ).when(yarnRunner).createApplicationSubmissionContext(
            any(String.class), any(Credentials.class));

    appId = recordFactory.newRecordInstance(ApplicationId.class);
    appId.setClusterTimestamp(System.currentTimeMillis());
    appId.setId(1);
    jobId = new JobId(appId);
    if (testWorkDir.exists()) {
      FileContext.getLocalFSFileContext().delete(new Path(testWorkDir.toString()), true);
    }
    testWorkDir.mkdirs();
   }

  @Test
  public void testResourceMgrDelegate() throws Exception {
    /* we not want a mock of resourcemgr deleagte */
    ClientRMProtocol clientRMProtocol = mock(ClientRMProtocol.class);
    ResourceMgrDelegate delegate = new ResourceMgrDelegate(conf, clientRMProtocol);
    /* make sure kill calls finish application master */
    when(clientRMProtocol.forceKillApplication(any(KillApplicationRequest.class)))
    .thenReturn(null);
    delegate.killApplication(appId);
    verify(clientRMProtocol).forceKillApplication(any(KillApplicationRequest.class));


    /* make sure getapplication report is called */
    when(clientRMProtocol.getApplicationReport(any(GetApplicationReportRequest.class)))
    .thenReturn(recordFactory.newRecordInstance(GetApplicationReportResponse.class));
    delegate.getApplicationReport(appId);
    verify(clientRMProtocol).getApplicationReport(any(GetApplicationReportRequest.class));

    /* make sure metrics is called */
    GetClusterMetricsResponse clusterMetricsResponse = recordFactory.newRecordInstance
        (GetClusterMetricsResponse.class);
    clusterMetricsResponse.setClusterMetrics(recordFactory.newRecordInstance(
        YarnClusterMetrics.class));
    when(clientRMProtocol.getClusterMetrics(any(GetClusterMetricsRequest.class)))
    .thenReturn(clusterMetricsResponse);
    delegate.getClusterMetrics();
    verify(clientRMProtocol).getClusterMetrics(any(GetClusterMetricsRequest.class));

    
    GetNewApplicationResponse newAppResponse = recordFactory.newRecordInstance(
        GetNewApplicationResponse.class);
    newAppResponse.setApplicationId(appId);
    when(clientRMProtocol.getNewApplication(any(GetNewApplicationRequest.class))).
    thenReturn(newAppResponse);
    delegate.getNewJobID();
    verify(clientRMProtocol).getNewApplication(any(GetNewApplicationRequest.class));
    

  }
}
