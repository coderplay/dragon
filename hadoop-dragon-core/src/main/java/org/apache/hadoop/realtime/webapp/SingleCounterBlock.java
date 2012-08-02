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
import org.apache.hadoop.realtime.job.Task;
import org.apache.hadoop.realtime.job.TaskAttempt;
import org.apache.hadoop.realtime.records.*;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TR;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import java.util.Map;
import java.util.TreeMap;

import static org.apache.hadoop.realtime.webapp.DragonParams.*;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._INFO_WRAP;

public class SingleCounterBlock extends HtmlBlock {
  protected TreeMap<String, Long> values = new TreeMap<String, Long>(); 
  protected Job job;
  protected Task task;
  
  @Inject SingleCounterBlock(AppContext appCtx, ViewContext ctx) {
    super(ctx);
    this.populateMembers(appCtx);
  }

  @Override protected void render(Block html) {
    if (job == null) {
      html.
        p()._("Sorry, no counters for nonexistent", $(JOB_ID, "job"))._();
      return;
    }
    if (!$(TASK_ID).isEmpty() && task == null) {
      html.
        p()._("Sorry, no counters for nonexistent", $(TASK_ID, "task"))._();
      return;
    }
    
    String columnType = task == null ? "Task" : "Task Attempt";
    
    TBODY<TABLE<DIV<Hamlet>>> tbody = html.
      div(_INFO_WRAP).
      table("#singleCounter").
        thead().
          tr().
            th(".ui-state-default", columnType).
            th(".ui-state-default", "Value")._()._().
          tbody();
    for (Map.Entry<String, Long> entry : values.entrySet()) {
      TR<TBODY<TABLE<DIV<Hamlet>>>> row = tbody.tr();
      String id = entry.getKey();
      String val = entry.getValue().toString();
      if(task != null) {
        row.td(id);
        row.td().br().$title(val)._()._(val)._();
      } else {
        row.td().a(url("singletaskcounter",entry.getKey(),
            $(COUNTER_GROUP), $(COUNTER_NAME)), id)._();
        row.td().br().$title(val)._().a(url("singletaskcounter",entry.getKey(),
            $(COUNTER_GROUP), $(COUNTER_NAME)), val)._();
      }
      row._();
    }
    tbody._()._()._();
  }

  private void populateMembers(AppContext ctx) {
    JobId jobID = null;
    TaskId taskID = null;
    String tid = $(TASK_ID);
    if (!tid.isEmpty()) {
      taskID = TaskId.parseTaskId(tid);
      jobID = taskID.getJobId();
    } else {
      String jid = $(JOB_ID);
      if (!jid.isEmpty()) {
        jobID = JobId.parseJobId(jid);
      }
    }
    if (jobID == null) {
      return;
    }
    job = ctx.getJob(jobID);
    if (job == null) {
      return;
    }
    if (taskID != null) {
      task = job.getTask(taskID);
      if (task == null) {
        return;
      }
      for(Map.Entry<TaskAttemptId, TaskAttempt> entry :
        task.getAttempts().entrySet()) {
        long value = 0;
        Counters counters = entry.getValue().getCounters();
        CounterGroup group = (counters != null) ? counters
          .getCounterGroup($(COUNTER_GROUP)) : null;
        if(group != null)  {
          Counter c = group.getCounter($(COUNTER_NAME));
          if(c != null) {
            value = c.getValue();
          }
        }
        values.put(entry.getKey().toString(), value);
      }
      
      return;
    }
    // Get all types of counters
    Map<TaskId, Task> tasks = job.getTasks();
    for(Map.Entry<TaskId, Task> entry : tasks.entrySet()) {
      long value = 0;
      CounterGroup group = entry.getValue().getCounters()
        .getCounterGroup($(COUNTER_GROUP));
      if(group != null)  {
        Counter c = group.getCounter($(COUNTER_NAME));
        if(c != null) {
          value = c.getValue();
        }
      }
      values.put(entry.getKey().toString(), value);
    }
  }
}
