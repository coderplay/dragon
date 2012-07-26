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
package org.apache.hadoop.realtime.util;

import org.junit.Test;

import java.util.Iterator;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

/**
 * test suite of LRUMap
 */
public class LRUMapTest {
  @Test
  public void testPutAndGet() {
    LRUMap<String,String> map = new LRUMap<String, String>(3);
    map.put ("1", "one");                           // 1
    map.put ("2", "two");                           // 2 1
    map.put ("3", "three");                         // 3 2 1
    map.put ("4", "four");                          // 4 3 2
    assertEquals("two", map.get("2"));              // 2 4 3
    assertNull(map.get("1"));

    map.put ("5", "five");                          // 5 2 4
    map.put ("4", "second four");                   // 4 5 2

    assertEquals(3, map.usedEntries());
    assertEquals("second four", map.get("4"));
    assertEquals("five", map.get("5"));
    assertEquals("two", map.get("2"));
  }

  @Test
  public void testConvertToBytes() {
    LRUMap<String,String> map = new LRUMap<String, String>(3);
    map.put ("1", "one");                           // 1
    map.put ("2", "two");                           // 2 1
    map.put ("3", "three");                         // 3 2 1
    map.put ("4", "four");                          // 4 3 2

    byte[] bytes = map.toByteArray();
    LRUMap<String,String> newMap = LRUMap.fromByteArray(bytes);

    assertEquals(map.usedEntries(), newMap.usedEntries());

    Iterator<Map.Entry<String,String>> mapIt = map.entrySet().iterator();
    Iterator<Map.Entry<String,String>> newMapIt = newMap.entrySet().iterator();
    while(mapIt.hasNext()) {
      Map.Entry<String,String> entry = mapIt.next();
      Map.Entry<String,String> entryNew = newMapIt.next();

      assertEquals(entry.getKey(), entryNew.getKey());
      assertEquals(entry.getValue(), entryNew.getValue());
    }
  }

}
