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
 * An event or a repeated event for an object that is registered with a {@link
 * DefaultWatchService}.
 *
 * <p> An event is classified by its {@link #kind() kind} and has a {@link
 * #count() count} to indicate the number of times that the event has been
 * observed. This allows for efficient representation of repeated events. The
 * {@link #context() context} method returns any context associated with
 * the event. In the case of a repeated event then the context is the same for
 * all events.
 *
 * <p> Watch events are immutable and safe for use by multiple concurrent
 * threads.
 *
 * @param   <T>     The type of the context object associated with the event
 *
 * @since 1.7
 */

public interface WatchEvent<T> {

    /**
     * An event kind, for the purposes of identification.
     *
     * @since 1.7
     * @see StandardWatchEventKinds
     */
    public static interface Kind<T> {
        /**
         * Returns the name of the event kind.
         */
        String name();

        /**
         * Returns the type of the {@link WatchEvent#context context} value.
         */
        Class<T> type();
    }

    /**
     * Returns the event kind.
     *
     * @return  the event kind
     */
    Kind<T> kind();

    /**
     * Returns the event count. If the event count is greater than {@code 1}
     * then this is a repeated event.
     *
     * @return  the event count
     */
    int count();

    /**
     * Returns the context for the event.
     *
     * <p> In the case of {@link StandardWatchEventKinds#ENTRY_CREATE ENTRY_CREATE},
     * {@link StandardWatchEventKinds#ENTRY_DELETE ENTRY_DELETE}, and {@link
     * StandardWatchEventKinds#ENTRY_MODIFY ENTRY_MODIFY} events the context is
     * a {@code Path} that is the {@link Path#relativize relative} path between
     * the directory registered with the watch service, and the entry that is
     * created, deleted, or modified.
     *
     * @return  the event context; may be {@code null}
     */
    T context();
}