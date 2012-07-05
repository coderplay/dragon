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
import com.esotericsoftware.kryo.io.Input;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

/**
 * class description goes here.
 *
 * @author xiaofeng_metis
 */
public class EventReader implements Closeable {
  private static final Log LOG = LogFactory.getLog(EventReader.class);

  private Input input;
  private Kryo kryo;

  public EventReader(final FileSystem fs, final Path name) throws IOException {
    this(fs.open(name));
  }

  @VisibleForTesting
  EventReader(final InputStream inputStream) {
    this.input = new Input(inputStream);
    this.kryo = KryoUtils.createHistoryEventKryo();
  }

  public boolean hasNext() throws IOException {
    return input.canReadInt();
  }

  public HistoryEvent nextEvent() throws IOException {
    return (HistoryEvent) this.kryo.readClassAndObject(this.input);
  }

  @Override
  public void close() throws IOException {
    try {
      input.close();
      input = null;
    } finally {
      IOUtils.cleanup(LOG, input);
    }
  }
}
