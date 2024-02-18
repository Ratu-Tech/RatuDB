/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.support;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;

public class ContextPreservingActionListenerTests extends OpenSearchTestCase {

    public void testOriginalContextIsPreservedAfterOnResponse() throws IOException {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final boolean nonEmptyContext = randomBoolean();
        if (nonEmptyContext) {
            threadContext.putHeader("not empty", "value");
        }
        final ContextPreservingActionListener<Void> actionListener;
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.putHeader("foo", "bar");
            final ActionListener<Void> delegate = new ActionListener<Void>() {
                @Override
                public void onResponse(Void aVoid) {
                    assertEquals("bar", threadContext.getHeader("foo"));
                    assertNull(threadContext.getHeader("not empty"));
                }

                @Override
                public void onFailure(Exception e) {
                    throw new RuntimeException("onFailure shouldn't be called", e);
                }
            };
            if (randomBoolean()) {
                actionListener = new ContextPreservingActionListener<>(threadContext.newRestorableContext(true), delegate);
            } else {
                actionListener = ContextPreservingActionListener.wrapPreservingContext(delegate, threadContext);
            }
        }

        assertNull(threadContext.getHeader("foo"));
        assertEquals(nonEmptyContext ? "value" : null, threadContext.getHeader("not empty"));

        actionListener.onResponse(null);

        assertNull(threadContext.getHeader("foo"));
        assertEquals(nonEmptyContext ? "value" : null, threadContext.getHeader("not empty"));
    }

    public void testOriginalContextIsPreservedAfterOnFailure() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final boolean nonEmptyContext = randomBoolean();
        if (nonEmptyContext) {
            threadContext.putHeader("not empty", "value");
        }
        final ContextPreservingActionListener<Void> actionListener;
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.putHeader("foo", "bar");
            final ActionListener<Void> delegate = new ActionListener<Void>() {
                @Override
                public void onResponse(Void aVoid) {
                    throw new RuntimeException("onResponse shouldn't be called");
                }

                @Override
                public void onFailure(Exception e) {
                    assertEquals("bar", threadContext.getHeader("foo"));
                    assertNull(threadContext.getHeader("not empty"));
                }
            };

            if (randomBoolean()) {
                actionListener = new ContextPreservingActionListener<>(threadContext.newRestorableContext(true), delegate);
            } else {
                actionListener = ContextPreservingActionListener.wrapPreservingContext(delegate, threadContext);
            }

        }

        assertNull(threadContext.getHeader("foo"));
        assertEquals(nonEmptyContext ? "value" : null, threadContext.getHeader("not empty"));

        actionListener.onFailure(null);

        assertNull(threadContext.getHeader("foo"));
        assertEquals(nonEmptyContext ? "value" : null, threadContext.getHeader("not empty"));
    }

    public void testOriginalContextIsWhenListenerThrows() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final boolean nonEmptyContext = randomBoolean();
        if (nonEmptyContext) {
            threadContext.putHeader("not empty", "value");
        }
        final ContextPreservingActionListener<Void> actionListener;
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.putHeader("foo", "bar");
            final ActionListener<Void> delegate = new ActionListener<Void>() {
                @Override
                public void onResponse(Void aVoid) {
                    assertEquals("bar", threadContext.getHeader("foo"));
                    assertNull(threadContext.getHeader("not empty"));
                    throw new RuntimeException("onResponse called");
                }

                @Override
                public void onFailure(Exception e) {
                    assertEquals("bar", threadContext.getHeader("foo"));
                    assertNull(threadContext.getHeader("not empty"));
                    throw new RuntimeException("onFailure called");
                }
            };

            if (randomBoolean()) {
                actionListener = new ContextPreservingActionListener<>(threadContext.newRestorableContext(true), delegate);
            } else {
                actionListener = ContextPreservingActionListener.wrapPreservingContext(delegate, threadContext);
            }
        }

        assertNull(threadContext.getHeader("foo"));
        assertEquals(nonEmptyContext ? "value" : null, threadContext.getHeader("not empty"));

        RuntimeException e = expectThrows(RuntimeException.class, () -> actionListener.onResponse(null));
        assertEquals("onResponse called", e.getMessage());

        assertNull(threadContext.getHeader("foo"));
        assertEquals(nonEmptyContext ? "value" : null, threadContext.getHeader("not empty"));

        e = expectThrows(RuntimeException.class, () -> actionListener.onFailure(null));
        assertEquals("onFailure called", e.getMessage());

        assertNull(threadContext.getHeader("foo"));
        assertEquals(nonEmptyContext ? "value" : null, threadContext.getHeader("not empty"));
    }

    public void testToStringIncludesDelegate() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final ContextPreservingActionListener<Void> actionListener;
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            final ActionListener<Void> delegate = new ActionListener<Void>() {
                @Override
                public void onResponse(Void aVoid) {}

                @Override
                public void onFailure(Exception e) {}

                @Override
                public String toString() {
                    return "test delegate";
                }
            };

            actionListener = ContextPreservingActionListener.wrapPreservingContext(delegate, threadContext);
        }

        assertThat(actionListener.toString(), allOf(containsString("test delegate"), containsString("ContextPreservingActionListener")));
    }

}
