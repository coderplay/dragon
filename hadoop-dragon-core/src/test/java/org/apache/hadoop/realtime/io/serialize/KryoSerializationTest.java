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
package org.apache.hadoop.realtime.io.serialize;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.realtime.event.Event;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 */
public class KryoSerializationTest {

  private static Configuration conf;

  @Before
  public void setUp() {
    conf = new Configuration();
    conf.setStrings("io.serializations",
        "org.apache.hadoop.realtime.io.serializer.KryoSerialization");
  }

  @Test
  public void testSerialization() throws Exception {
    Event<String, String> before = new Event<String, String>() {
      private static final long serialVersionUID = 1L;
      // 3 fields
      private final long offset = 0L;
      private final String hello = "hello";
      private final String world = "world";
      @Override
      public long offset() { return offset; }

      @Override
      public String key() { return hello; }

      @Override
      public String value() {return world; }

    };

    Event<String, String> after =
        SerializationTestUtil.testSerialization(conf, before);
    Assert.assertEquals(before.key(), after.key());
    Assert.assertEquals(before.value(), after.value());
    Assert.assertEquals(before.offset(), after.offset());
  }
}
