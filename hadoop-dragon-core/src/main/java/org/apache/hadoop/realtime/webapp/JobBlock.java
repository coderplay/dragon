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
import org.apache.hadoop.realtime.DragonApps;
import org.apache.hadoop.realtime.client.app.AppContext;
import org.apache.hadoop.realtime.job.Job;
import org.apache.hadoop.realtime.records.AMInfo;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import java.util.Date;
import java.util.List;

import static org.apache.hadoop.realtime.webapp.DragonParams.JOB_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.*;
import org.apache.hadoop.realtime.DragonApps.TaskAttemptStateUI;

public class JobBlock extends HtmlBlock {
  final AppContext appContext;

  @Inject
  JobBlock(AppContext appctx) {
    appContext = appctx;
  }

  @Override protected void render(Block html) {
    String jid = $(JOB_ID);
    if (jid.isEmpty()) {
      html.
        p()._("Sorry, can't do anything without a JobID.")._();
      return;
    }
    JobId jobID = DragonApps.toJobID(jid);
    Job job = appContext.getJob(jobID);
    if (job == null) {
      html.
        p()._("Sorry, ", jid, " not found.")._();
      return;
    }

    List<AMInfo> amInfos = job.getAMInfos();
    String amString =
        amInfos.size() == 1 ? "ApplicationMaster" : "ApplicationMasters"; 

    JobInfo jinfo = new JobInfo(job, true);
    info("Job Overview").
        _("Job Name:", jinfo.getName()).
        _("State:", jinfo.getState()).
        _("Uberized:", jinfo.isUberized()).
        _("Started:", new Date(jinfo.getStartTime())).
        _("Elapsed:", StringUtils.formatTime(jinfo.getElapsedTime()));
    DIV<Hamlet> div = html.
      _(InfoBlock.class).
      div(_INFO_WRAP);

    // DragonAppMasters Table
    TABLE<DIV<Hamlet>> table = div.table("#job");
    table.
      tr().
      th(amString).
      _().
      tr().
      th(_TH, "Attempt Number").
      th(_TH, "Start Time").
      th(_TH, "Node").
      th(_TH, "Logs").
      _();
    for (AMInfo amInfo : amInfos) {
      AMAttemptInfo attempt = new AMAttemptInfo(amInfo,
          jinfo.getId(), jinfo.getUserName());

      table.tr().
        td(String.valueOf(attempt.getAttemptId())).
        td(new Date(attempt.getStartTime()).toString()).
        td().a(".nodelink", url("http://", attempt.getNodeHttpAddress()), 
            attempt.getNodeHttpAddress())._().
        td().a(".logslink", url(attempt.getLogsLink()), 
            "logs")._().
        _();
    }

    table._();
    div._();

    html.div(_INFO_WRAP).        
      // Tasks table
        table("#tasks").
          tr().
            th(_TH, "Task Type").
            th(_TH, "Total")._().
          tr().
            th().
              a(url("tasks", jid, "m"), "Map")._().
            td(String.valueOf(jinfo.getTotalTask()))._()
          ._().

        // Attempts table
        table("#attempts").
        tr().
          th(_TH, "Attempt Type").
          th(_TH, "New").
          th(_TH, "Running").
          th(_TH, "Failed").
          th(_TH, "Killed")._().
        tr().
          th("Maps").
          td().a(url("attempts", jid, "m",
              TaskAttemptStateUI.NEW.toString()),
              String.valueOf(jinfo.getNewAttempts()))._().
          td().a(url("attempts", jid, "m",
              TaskAttemptStateUI.RUNNING.toString()),
              String.valueOf(jinfo.getRunningAttempts()))._().
          td().a(url("attempts", jid, "m",
              TaskAttemptStateUI.FAILED.toString()),
              String.valueOf(jinfo.getFailedAttempts()))._().
          td().a(url("attempts", jid, "m",
              TaskAttemptStateUI.KILLED.toString()),
              String.valueOf(jinfo.getKilledAttempts()))._().
        _().
       _().
     _();

    html.div("#dag")._();
  }
}