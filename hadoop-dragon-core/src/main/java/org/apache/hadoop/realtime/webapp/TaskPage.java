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
import org.apache.hadoop.realtime.job.TaskAttempt;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TD;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TR;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import java.util.Collection;

import static org.apache.hadoop.yarn.util.StringHelper.percent;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.*;

public class TaskPage extends AppView {

  static class AttemptsBlock extends HtmlBlock {
    final App app;

    @Inject
    AttemptsBlock(App ctx) {
      app = ctx;
    }

    @Override
    protected void render(Block html) {
      if (!isValidRequest()) {
        html.
          h2($(TITLE));
        return;
      }
      TBODY<TABLE<Hamlet>> tbody = html.
      table("#attempts").
        thead().
          tr().
            th(".id", "Attempt").
            th(".progress", "Progress").
            th(".state", "State").
            th(".node", "node").
            th(".tsh", "Started").
            th(".tsh", "Finished").
            th(".tsh", "Elapsed").
            th(".note", "Note")._()._().
        tbody();
      for (TaskAttempt attempt : getTaskAttempts()) {
        TaskAttemptInfo ta = new TaskAttemptInfo(attempt, true);
        String taid = ta.getId();
        String progress = percent(ta.getProgress() / 100);
        ContainerId containerId = ta.getAssignedContainerId();

        String nodeHttpAddr = ta.getNode();
        long startTime = ta.getStartTime();
        long finishTime = ta.getFinishTime();
        long elapsed = ta.getElapsedTime();
        String diag = ta.getNote() == null ? "" : ta.getNote();
        TD<TR<TBODY<TABLE<Hamlet>>>> nodeTd = tbody.
          tr().
            td(".id", taid).
            td(".progress", progress).
            td(".state", ta.getState()).
            td().
              a(".nodelink", url("http://", nodeHttpAddr), nodeHttpAddr);
        if (containerId != null) {
          String containerIdStr = ta.getAssignedContainerIdStr();
          nodeTd._(" ").
            a(".logslink", url("http://", nodeHttpAddr, "node", "containerlogs",
              containerIdStr, app.getJob().getUserName()), "logs");
        }
        nodeTd._().
          td(".ts", Times.format(startTime)).
          td(".ts", Times.format(finishTime)).
          td(".dt", StringUtils.formatTime(elapsed)).
          td(".note", diag)._();
      }
      tbody._()._();
    }

    protected boolean isValidRequest() {
      return app.getTask() != null;
    }

    protected Collection<TaskAttempt> getTaskAttempts() {
      return app.getTask().getAttempts().values();
    }
  }

  @Override protected void preHead(Page.HTML<_> html) {
    commonPreHead(html);

    // This page is a list of all attempts which are limited in number. Okay to
    // refresh automatically.
    html.meta_http("refresh", "10");

    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:3}");
    set(DATATABLES_ID, "attempts");
    set(initID(DATATABLES, "attempts"), attemptsTableInit());
    setTableStyles(html, "attempts");
  }

  @Override protected Class<? extends SubView> content() {
    return AttemptsBlock.class;
  }

  private String attemptsTableInit() {
    return tableInit().
        // Sort by id upon page load
        append(", aaSorting: [[0, 'asc']]").
        append("}").toString();
  }
}
