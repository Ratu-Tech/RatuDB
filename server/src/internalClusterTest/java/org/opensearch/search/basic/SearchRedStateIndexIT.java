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

package org.opensearch.search.basic;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.search.SearchService;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedOpenSearchIntegTestCase;
import org.junit.After;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

@OpenSearchIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class SearchRedStateIndexIT extends ParameterizedOpenSearchIntegTestCase {

    public SearchRedStateIndexIT(Settings dynamicSettings) {
        super(dynamicSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.CONCURRENT_SEGMENT_SEARCH, "true").build();
    }

    public void testAllowPartialsWithRedState() throws Exception {
        final int numShards = cluster().numDataNodes() + 2;
        buildRedIndex(numShards);

        SearchResponse searchResponse = client().prepareSearch().setSize(0).setAllowPartialSearchResults(true).get();
        assertThat(RestStatus.OK, equalTo(searchResponse.status()));
        assertThat("Expect no shards failed", searchResponse.getFailedShards(), equalTo(0));
        assertThat("Expect no shards skipped", searchResponse.getSkippedShards(), equalTo(0));
        assertThat("Expect subset of shards successful", searchResponse.getSuccessfulShards(), lessThan(numShards));
        assertThat("Expected total shards", searchResponse.getTotalShards(), equalTo(numShards));
    }

    public void testClusterAllowPartialsWithRedState() throws Exception {
        final int numShards = cluster().numDataNodes() + 2;
        buildRedIndex(numShards);

        setClusterDefaultAllowPartialResults(true);

        SearchResponse searchResponse = client().prepareSearch().setSize(0).get();
        assertThat(RestStatus.OK, equalTo(searchResponse.status()));
        assertThat("Expect no shards failed", searchResponse.getFailedShards(), equalTo(0));
        assertThat("Expect no shards skipped", searchResponse.getSkippedShards(), equalTo(0));
        assertThat("Expect subset of shards successful", searchResponse.getSuccessfulShards(), lessThan(numShards));
        assertThat("Expected total shards", searchResponse.getTotalShards(), equalTo(numShards));
    }

    public void testDisallowPartialsWithRedState() throws Exception {
        buildRedIndex(cluster().numDataNodes() + 2);

        SearchPhaseExecutionException ex = expectThrows(
            SearchPhaseExecutionException.class,
            () -> client().prepareSearch().setSize(0).setAllowPartialSearchResults(false).get()
        );
        assertThat(ex.getDetailedMessage(), containsString("Search rejected due to missing shard"));
    }

    public void testClusterDisallowPartialsWithRedState() throws Exception {
        buildRedIndex(cluster().numDataNodes() + 2);

        setClusterDefaultAllowPartialResults(false);
        SearchPhaseExecutionException ex = expectThrows(
            SearchPhaseExecutionException.class,
            () -> client().prepareSearch().setSize(0).get()
        );
        assertThat(ex.getDetailedMessage(), containsString("Search rejected due to missing shard"));
    }

    private void setClusterDefaultAllowPartialResults(boolean allowPartialResults) {
        String key = SearchService.DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS.getKey();

        Settings transientSettings = Settings.builder().put(key, allowPartialResults).build();

        ClusterUpdateSettingsResponse response1 = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(transientSettings)
            .get();

        assertAcked(response1);
        assertEquals(response1.getTransientSettings().getAsBoolean(key, null), allowPartialResults);
    }

    private void buildRedIndex(int numShards) throws Exception {
        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder().put("index.number_of_shards", numShards).put("index.number_of_replicas", 0)
            )
        );
        ensureGreen();
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test").setId("" + i).setSource("field1", "value1").get();
        }
        refresh();

        internalCluster().stopRandomDataNode();

        client().admin().cluster().prepareHealth().setWaitForStatus(ClusterHealthStatus.RED).get();

        assertBusy(() -> {
            ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
            List<ShardRouting> unassigneds = clusterState.getRoutingTable().shardsWithState(ShardRoutingState.UNASSIGNED);
            assertThat(unassigneds.size(), greaterThan(0));
        });

    }

    @After
    public void cleanup() throws Exception {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().putNull(SearchService.DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS.getKey()))
        );
    }
}
