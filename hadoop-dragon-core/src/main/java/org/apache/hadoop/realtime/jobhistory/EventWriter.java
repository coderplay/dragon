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
package org.apache.hadoop.realtime.jobhistory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;

/**
 * class description goes here.
 */
class EventWriter {
  private static final Log LOG = LogFactory.getLog(EventWriter.class);

  private Kryo kryo;
  private Output output;
  private FSDataOutputStream out;

  EventWriter(FSDataOutputStream out) {
    this.out = out;
    this.output = new Output(out);
    this.kryo = KryoUtils.createHistoryEventKryo();
  }

  synchronized void write(HistoryEvent event) throws IOException {
    this.kryo.writeClassAndObject(this.output, event);
  }

  void flush() throws IOException {
    this.output.flush();
    this.out.hflush();
  }

  void close() throws IOException {
    try {
      output.close();

      output = null;
      out = null;
      kryo = null;
    } finally {
      IOUtils.cleanup(LOG, output);
    }
  }
}
