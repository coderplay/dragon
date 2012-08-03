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
import org.apache.hadoop.realtime.client.app.AppContext;
import org.apache.hadoop.realtime.job.Job;
import org.apache.hadoop.realtime.webapp.dao.JobInfo;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._PROGRESSBAR;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._PROGRESSBAR_VALUE;

public class JobsBlock extends HtmlBlock {
  final AppContext appContext;

  @Inject JobsBlock(AppContext appCtx) {
    appContext = appCtx;
  }

  @Override protected void render(Block html) {
    TBODY<TABLE<Hamlet>> tbody = html.
      h2("Active Jobs").
      table("#jobs").
        thead().
          tr().
            th(".id", "Job ID").
            th(".name", "Name").
            th(".state", "State").
            th("Maps Total").
            th("Reduces Total").
            _()._().
        tbody();
    for (Job j : appContext.getAllJobs().values()) {
      JobInfo job = new JobInfo(j, false);
      tbody.
        tr().
          td().
            span().$title(String.valueOf(job.getId()))._(). // for sorting
            a(url("job", job.getId()), job.getId())._().
          td(job.getName()).
          td(job.getState()).
          td(String.valueOf(job.getMapsTotal())).
          td(String.valueOf(job.getReducesTotal())).
          _();
    }
    tbody._()._();
  }
}
