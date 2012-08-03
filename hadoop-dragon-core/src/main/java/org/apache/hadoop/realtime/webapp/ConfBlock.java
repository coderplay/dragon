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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.realtime.client.app.AppContext;
import org.apache.hadoop.realtime.job.Job;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.webapp.dao.ConfEntryInfo;
import org.apache.hadoop.realtime.webapp.dao.ConfInfo;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.InputType;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import java.io.IOException;

import static org.apache.hadoop.realtime.webapp.DragonParams.JOB_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._TH;

/**
 * Render the configuration for this job.
 */
public class ConfBlock extends HtmlBlock {
  final AppContext appContext;
  final Configuration conf;

  @Inject
  ConfBlock(AppContext appctx, Configuration conf) {
    appContext = appctx;
    this.conf = conf;
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.yarn.webapp.view.HtmlBlock#render(org.apache.hadoop.yarn.webapp.view.HtmlBlock.Block)
   */
  @Override protected void render(Block html) {
    String jid = $(JOB_ID);
    if (jid.isEmpty()) {
      html.
        p()._("Sorry, can't do anything without a JobID.")._();
      return;
    }
    JobId jobID = JobId.parseJobId(jid);
    Job job = appContext.getJob(jobID);
    if (job == null) {
      html.
        p()._("Sorry, ", jid, " not found.")._();
      return;
    }
    Path confPath = job.getConfFile();
    try {
      ConfInfo info = new ConfInfo(job, this.conf);

      html.div().h3(confPath.toString())._();
      TBODY<TABLE<Hamlet>> tbody = html.
        // Tasks table
      table("#conf").
        thead().
          tr().
            th(_TH, "key").
            th(_TH, "value").
          _().
        _().
      tbody();
      for (ConfEntryInfo entry : info.getProperties()) {
        tbody.
          tr().
            td(entry.getName()).
            td(entry.getValue()).
          _();
      }
      tbody._().
      tfoot().
        tr().
          th().input("search_init").$type(InputType.text).$name("key").$value("key")._()._().
          th().input("search_init").$type(InputType.text).$name("value").$value("value")._()._().
          _().
        _().
      _();
    } catch(IOException e) {
      LOG.error("Error while reading "+confPath, e);
      html.p()._("Sorry got an error while reading conf file. ",confPath);
    }
  }
}
