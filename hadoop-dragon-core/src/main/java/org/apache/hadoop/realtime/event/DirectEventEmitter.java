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
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;

/**
 */
public class DirectEventEmitter<KEY, VALUE> implements
    EventEmitter<KEY, VALUE> {
  Configuration conf;
  FSDataOutputStream out;
  protected Serializer<? extends Event<KEY, VALUE>> serializer;

  public DirectEventEmitter(final FileSystem fs, final Configuration conf,
      final Path name, final Class<? extends Event<KEY, VALUE>> eventClass)
      throws IOException {
    this(fs, conf, name, eventClass, fs.getConf().getInt("io.file.buffer.size",
        4096), fs.getDefaultReplication(), fs.getDefaultBlockSize());
  }

  public DirectEventEmitter(final FileSystem fs, final Configuration conf,
      final Path name, final Class<? extends Event<KEY, VALUE>> eventClass,
      final int bufferSize, final short replication, final long blockSize)
      throws IOException {
    init(name, conf, fs.create(name, true, bufferSize, replication, blockSize),
        eventClass);
  }

  void init(final Path name, final Configuration conf,
      final FSDataOutputStream out,
      final Class<? extends Event<KEY, VALUE>> eventClass) {
    SerializationFactory sf = new SerializationFactory(conf);
    this.serializer = sf.getSerializer(eventClass);
  }

  @Override
  public boolean emitEvent(Event<KEY, VALUE> event) throws IOException,
      InterruptedException {
    
    return false;
  }

  @Override
  public boolean
      emitEvent(Event<KEY, VALUE> event, long timeout, TimeUnit unit)
          throws IOException, InterruptedException {
    return false;
  }

  @Override
  public void close() throws IOException {

  }

}
