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
package org.apache.hadoop.realtime.webapp;

import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.realtime.DragonApps;
import org.apache.hadoop.realtime.job.Job;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.webapp.Controller;

import javax.servlet.http.HttpServletResponse;

import static org.apache.hadoop.yarn.util.StringHelper.join;

/**
 * This class renders the various pages that the web app supports.
 */
public class AppController extends Controller implements DragonParams {

  protected final App app;

  protected AppController(App app, Configuration conf, RequestContext ctx,
                          String title) {
    super(ctx);
    this.app = app;
    set(APP_ID, app.context.getApplicationID().toString());
    set(RM_WEB, YarnConfiguration.getRMWebAppURL(conf));
  }

  @Inject
  protected AppController(App app, Configuration conf, RequestContext ctx) {
    this(app, conf, ctx, "am");
  }

  /**
   * Render the default(index.html) page for the Application Controller
   */
  @Override public void index() {
    setTitle(join("MapReduce Application ", $(APP_ID)));
  }

  /**
   * Render the /job page
   */
  public void job() {
    try {
      requireJob();
    }
    catch (Exception e) {
      renderText(e.getMessage());
      return;
    }
    render(JobPage.class);
  }

  /**
   * Ensure that a JOB_ID was passed into the page.
   */
  public void requireJob() {
    if ($(JOB_ID).isEmpty()) {
      badRequest("missing job ID");
      throw new RuntimeException("Bad Request: Missing job ID");
    }

    JobId jobID = DragonApps.toJobID($(JOB_ID));
    app.setJob(app.context.getJob(jobID));
    if (app.getJob() == null) {
      notFound($(JOB_ID));
      throw new RuntimeException("Not Found: " + $(JOB_ID));
    }

    /* check for acl access */
    Job job = app.context.getJob(jobID);
    if (!checkAccess(job)) {
      accessDenied("User " + request().getRemoteUser() + " does not have " +
          " permission to view job " + $(JOB_ID));
      throw new RuntimeException("Access denied: User " +
          request().getRemoteUser() + " does not have permission to view job " +
          $(JOB_ID));
    }
  }

  /**
   * Render a BAD_REQUEST error.
   * @param s the error message to include.
   */
  void badRequest(String s) {
    setStatus(HttpServletResponse.SC_BAD_REQUEST);
    setTitle(join("Bad request: ", s));
  }

  /**
   * Render a NOT_FOUND error.
   * @param s the error message to include.
   */
  void notFound(String s) {
    setStatus(HttpServletResponse.SC_NOT_FOUND);
    setTitle(join("Not found: ", s));
  }

  /**
   * Render a ACCESS_DENIED error.
   * @param s the error message to include.
   */
  void accessDenied(String s) {
    setStatus(HttpServletResponse.SC_FORBIDDEN);
    setTitle(join("Access denied: ", s));
  }

  /**
   * check for job access.
   * @param job the job that is being accessed
   * @return True if the requesting user has permission to view the job
   */
  boolean checkAccess(Job job) {
    return true;
    /*UserGroupInformation callerUgi = UserGroupInformation.createRemoteUser(
        request().getRemoteUser());
    return job.checkAccess(callerUgi, JobACL.VIEW_JOB);   */
  }

}
