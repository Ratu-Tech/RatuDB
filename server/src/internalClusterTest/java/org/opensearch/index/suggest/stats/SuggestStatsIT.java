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

package org.opensearch.index.suggest.stats;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.search.stats.SearchStats;
import org.opensearch.search.suggest.SuggestBuilder;
import org.opensearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.opensearch.search.suggest.term.TermSuggestionBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedOpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAllSuccessful;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@OpenSearchIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class SuggestStatsIT extends ParameterizedOpenSearchIntegTestCase {

    public SuggestStatsIT(Settings dynamicSettings) {
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

    @Override
    protected int numberOfReplicas() {
        return 0;
    }

    public void testSimpleStats() throws Exception {
        // clear all stats first
        client().admin().indices().prepareStats().clear().execute().actionGet();
        final int numNodes = cluster().numDataNodes();
        assertThat(numNodes, greaterThanOrEqualTo(2));
        final int shardsIdx1 = randomIntBetween(1, 10); // we make sure each node gets at least a single shard...
        final int shardsIdx2 = Math.max(numNodes - shardsIdx1, randomIntBetween(1, 10));
        final int totalShards = shardsIdx1 + shardsIdx2;
        assertThat(numNodes, lessThanOrEqualTo(totalShards));
        assertAcked(
            prepareCreate("test1").setSettings(
                Settings.builder().put(SETTING_NUMBER_OF_SHARDS, shardsIdx1).put(SETTING_NUMBER_OF_REPLICAS, 0)
            ).setMapping("f", "type=text")
        );
        assertAcked(
            prepareCreate("test2").setSettings(
                Settings.builder().put(SETTING_NUMBER_OF_SHARDS, shardsIdx2).put(SETTING_NUMBER_OF_REPLICAS, 0)
            ).setMapping("f", "type=text")
        );
        assertThat(shardsIdx1 + shardsIdx2, equalTo(numAssignedShards("test1", "test2")));
        assertThat(numAssignedShards("test1", "test2"), greaterThanOrEqualTo(2));
        ensureGreen();

        for (int i = 0; i < randomIntBetween(20, 100); i++) {
            index("test" + ((i % 2) + 1), "type", "" + i, "f", "test" + i);
        }
        refresh();

        int suggestAllIdx = scaledRandomIntBetween(20, 50);
        int suggestIdx1 = scaledRandomIntBetween(20, 50);
        int suggestIdx2 = scaledRandomIntBetween(20, 50);

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < suggestAllIdx; i++) {
            SearchResponse suggestResponse = addSuggestions(internalCluster().coordOnlyNodeClient().prepareSearch(), i).get();
            assertAllSuccessful(suggestResponse);
        }
        for (int i = 0; i < suggestIdx1; i++) {
            SearchResponse suggestResponse = addSuggestions(internalCluster().coordOnlyNodeClient().prepareSearch("test1"), i).get();
            assertAllSuccessful(suggestResponse);
        }
        for (int i = 0; i < suggestIdx2; i++) {
            SearchResponse suggestResponse = addSuggestions(internalCluster().coordOnlyNodeClient().prepareSearch("test2"), i).get();
            assertAllSuccessful(suggestResponse);
        }
        long endTime = System.currentTimeMillis();

        IndicesStatsResponse indicesStats = client().admin().indices().prepareStats().execute().actionGet();
        final SearchStats.Stats suggest = indicesStats.getTotal().getSearch().getTotal();

        // check current
        assertThat(suggest.getSuggestCurrent(), equalTo(0L));

        // check suggest count
        assertThat(
            suggest.getSuggestCount(),
            equalTo((long) (suggestAllIdx * totalShards + suggestIdx1 * shardsIdx1 + suggestIdx2 * shardsIdx2))
        );
        assertThat(
            indicesStats.getIndices().get("test1").getTotal().getSearch().getTotal().getSuggestCount(),
            equalTo((long) ((suggestAllIdx + suggestIdx1) * shardsIdx1))
        );
        assertThat(
            indicesStats.getIndices().get("test2").getTotal().getSearch().getTotal().getSuggestCount(),
            equalTo((long) ((suggestAllIdx + suggestIdx2) * shardsIdx2))
        );

        logger.info("iter {}, iter1 {}, iter2 {}, {}", suggestAllIdx, suggestIdx1, suggestIdx2, endTime - startTime);
        // check suggest time
        assertThat(suggest.getSuggestTimeInMillis(), greaterThanOrEqualTo(0L));
        // the upperbound is num shards * total time since we do searches in parallel
        assertThat(suggest.getSuggestTimeInMillis(), lessThanOrEqualTo(totalShards * (endTime - startTime)));

        NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats().execute().actionGet();
        Set<String> nodeIdsWithIndex = nodeIdsWithIndex("test1", "test2");
        int num = 0;
        for (NodeStats stat : nodeStats.getNodes()) {
            SearchStats.Stats suggestStats = stat.getIndices().getSearch().getTotal();
            logger.info("evaluating {}", stat.getNode());
            if (nodeIdsWithIndex.contains(stat.getNode().getId())) {
                assertThat(suggestStats.getSuggestCount(), greaterThan(0L));
                assertThat(suggestStats.getSuggestTimeInMillis(), greaterThanOrEqualTo(0L));
                num++;
            } else {
                assertThat(suggestStats.getSuggestCount(), equalTo(0L));
                assertThat(suggestStats.getSuggestTimeInMillis(), equalTo(0L));
            }
        }

        assertThat(num, greaterThan(0));

    }

    private SearchRequestBuilder addSuggestions(SearchRequestBuilder request, int i) {
        final SuggestBuilder suggestBuilder = new SuggestBuilder();
        for (int s = 0; s < randomIntBetween(2, 10); s++) {
            if (randomBoolean()) {
                suggestBuilder.addSuggestion("s" + s, new PhraseSuggestionBuilder("f").text("test" + i + " test" + (i - 1)));
            } else {
                suggestBuilder.addSuggestion("s" + s, new TermSuggestionBuilder("f").text("test" + i));
            }
        }
        return request.suggest(suggestBuilder);
    }

    private Set<String> nodeIdsWithIndex(String... indices) {
        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        GroupShardsIterator<ShardIterator> allAssignedShardsGrouped = state.routingTable().allAssignedShardsGrouped(indices, true);
        Set<String> nodes = new HashSet<>();
        for (ShardIterator shardIterator : allAssignedShardsGrouped) {
            for (ShardRouting routing : shardIterator) {
                if (routing.active()) {
                    nodes.add(routing.currentNodeId());
                }

            }
        }
        return nodes;
    }

    protected int numAssignedShards(String... indices) {
        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        GroupShardsIterator allAssignedShardsGrouped = state.routingTable().allAssignedShardsGrouped(indices, true);
        return allAssignedShardsGrouped.size();
    }
}
