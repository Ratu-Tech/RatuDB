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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.cluster;

import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterApplierService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterStateObserverTests extends OpenSearchTestCase {

    public void testClusterStateListenerToStringIncludesListenerToString() {
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        final AtomicBoolean listenerAdded = new AtomicBoolean();

        doAnswer(invocation -> {
            assertThat(Arrays.toString(invocation.getArguments()), containsString("test-listener"));
            listenerAdded.set(true);
            return null;
        }).when(clusterApplierService).addTimeoutListener(any(), any());

        final ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(DiscoveryNodes.builder()).build();
        when(clusterApplierService.state()).thenReturn(clusterState);

        final ClusterStateObserver clusterStateObserver = new ClusterStateObserver(
            clusterState,
            clusterApplierService,
            null,
            logger,
            new ThreadContext(Settings.EMPTY)
        );
        clusterStateObserver.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {}

            @Override
            public void onClusterServiceClose() {}

            @Override
            public void onTimeout(TimeValue timeout) {}

            @Override
            public String toString() {
                return "test-listener";
            }
        });

        assertTrue(listenerAdded.get());
    }

}
