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
package org.apache.hadoop.realtime.fs;

import org.apache.hadoop.fs.Path;

/**
 * Defines the <em>standard</em> event kinds.
 */

public final class StandardWatchEventKinds {
  private StandardWatchEventKinds() {
  }

  /**
   * A special event to indicate that events may have been lost or discarded.
   * 
   * <p>
   * The {@link WatchEvent#context context} for this event is implementation
   * specific and may be {@code null}. The event {@link WatchEvent#count count}
   * may be greater than {@code 1}.
   * 
   * @see DefaultWatchService
   */
  public static final WatchEvent.Kind<Object> OVERFLOW =
      new StdWatchEventKind<Object>("OVERFLOW", Object.class);

  /**
   * Directory entry created.
   * 
   * <p>
   * When a directory is registered for this event then the
   * {@link DefaultWatchKey} is queued when it is observed that an entry is
   * created in the directory or renamed into the directory. The event
   * {@link WatchEvent#count count} for this event is always {@code 1}.
   */
  public static final WatchEvent.Kind<Path> ENTRY_CREATE =
      new StdWatchEventKind<Path>("ENTRY_CREATE", Path.class);

  /**
   * Directory entry deleted.
   * 
   * <p>
   * When a directory is registered for this event then the
   * {@link DefaultWatchKey} is queued when it is observed that an entry is
   * deleted or renamed out of the directory. The event {@link WatchEvent#count
   * count} for this event is always {@code 1}.
   */
  public static final WatchEvent.Kind<Path> ENTRY_DELETE =
      new StdWatchEventKind<Path>("ENTRY_DELETE", Path.class);

  /**
   * Directory entry modified.
   * 
   * <p>
   * When a directory is registered for this event then the
   * {@link DefaultWatchKey} is queued when it is observed that an entry in the
   * directory has been modified. The event {@link WatchEvent#count count} for
   * this event is {@code 1} or greater.
   */
  public static final WatchEvent.Kind<Path> ENTRY_MODIFY =
      new StdWatchEventKind<Path>("ENTRY_MODIFY", Path.class);

  private static class StdWatchEventKind<T> implements WatchEvent.Kind<T> {
    private final String name;
    private final Class<T> type;

    StdWatchEventKind(String name, Class<T> type) {
      this.name = name;
      this.type = type;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public Class<T> type() {
      return type;
    }

    @Override
    public String toString() {
      return name;
    }
  }
}
