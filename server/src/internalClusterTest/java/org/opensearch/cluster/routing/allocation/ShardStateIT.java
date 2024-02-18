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

package org.opensearch.cluster.routing.allocation;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.hamcrest.Matchers.equalTo;

public class ShardStateIT extends OpenSearchIntegTestCase {

    public void testPrimaryFailureIncreasesTerm() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        prepareCreate("test").setSettings(
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
        ).get();
        ensureGreen();
        assertPrimaryTerms(1, 1);

        logger.info("--> disabling allocation to capture shard failure");
        disableAllocation("test");

        ClusterState state = client().admin().cluster().prepareState().get().getState();
        final int shard = randomBoolean() ? 0 : 1;
        final String nodeId = state.routingTable().index("test").shard(shard).primaryShard().currentNodeId();
        final String node = state.nodes().get(nodeId).getName();
        logger.info("--> failing primary of [{}] on node [{}]", shard, node);
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, node);
        indicesService.indexService(resolveIndex("test")).getShard(shard).failShard("simulated test failure", null);

        logger.info("--> waiting for a yellow index");
        // we can't use ensureYellow since that one is just as happy with a GREEN status.
        assertBusy(
            () -> assertThat(client().admin().cluster().prepareHealth("test").get().getStatus(), equalTo(ClusterHealthStatus.YELLOW))
        );

        final long term0 = shard == 0 ? 2 : 1;
        final long term1 = shard == 1 ? 2 : 1;
        assertPrimaryTerms(term0, term1);

        logger.info("--> enabling allocation");
        enableAllocation("test");
        ensureGreen();
        assertPrimaryTerms(term0, term1);
    }

    protected void assertPrimaryTerms(long shard0Term, long shard1Term) {
        for (String node : internalCluster().getNodeNames()) {
            logger.debug("--> asserting primary terms terms on [{}]", node);
            ClusterState state = client(node).admin().cluster().prepareState().setLocal(true).get().getState();
            IndexMetadata metadata = state.metadata().index("test");
            assertThat(metadata.primaryTerm(0), equalTo(shard0Term));
            assertThat(metadata.primaryTerm(1), equalTo(shard1Term));
            IndicesService indicesService = internalCluster().getInstance(IndicesService.class, node);
            IndexService indexService = indicesService.indexService(metadata.getIndex());
            if (indexService != null) {
                for (IndexShard shard : indexService) {
                    assertThat(
                        "term mismatch for shard " + shard.shardId(),
                        shard.getPendingPrimaryTerm(),
                        equalTo(metadata.primaryTerm(shard.shardId().id()))
                    );
                }
            }
        }
    }
}
