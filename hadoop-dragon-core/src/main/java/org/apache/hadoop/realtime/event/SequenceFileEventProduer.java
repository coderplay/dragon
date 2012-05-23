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
package org.apache.hadoop.realtime.event;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.realtime.job.TaskAttempt;

/**
 * An {@link EventProducer} that continuously produce {@link Event}
 * from {@link SequenceFile}s by reading key, value pairs.
 */
public class SequenceFileEventProduer<KEY, VALUE> implements
    EventProducer<KEY, VALUE> {

  private SequenceFile.Reader in;
  protected Configuration conf;

  public SequenceFileEventProduer(Configuration conf, Path path)
      throws IOException {
    FileSystem fs = path.getFileSystem(conf);

  }

  public SequenceFileEventProduer(Configuration conf, Path path, long offset)
      throws IOException {

  }

  @Override
  public void initialize(TaskAttempt context) throws IOException,
      InterruptedException {

  }

  @Override
  public boolean nextEvent() throws IOException, InterruptedException {
    return false;
  }

  @Override
  public Event<KEY, VALUE> getCurrentEvent() throws IOException,
      InterruptedException {
    return null;
  }

  @Override
  public void close() throws IOException {
  }

}
