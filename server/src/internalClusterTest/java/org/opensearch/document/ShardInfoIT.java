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

package org.opensearch.document;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.indices.recovery.RecoveryResponse;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class ShardInfoIT extends OpenSearchIntegTestCase {
    private int numCopies;
    private int numNodes;

    public void testIndexAndDelete() throws Exception {
        prepareIndex(1);
        IndexResponse indexResponse = client().prepareIndex("idx").setSource("{}", MediaTypeRegistry.JSON).get();
        assertShardInfo(indexResponse);
        DeleteResponse deleteResponse = client().prepareDelete("idx", indexResponse.getId()).get();
        assertShardInfo(deleteResponse);
    }

    public void testUpdate() throws Exception {
        prepareIndex(1);
        UpdateResponse updateResponse = client().prepareUpdate("idx", "1").setDoc("{}", MediaTypeRegistry.JSON).setDocAsUpsert(true).get();
        assertShardInfo(updateResponse);
    }

    public void testBulkWithIndexAndDeleteItems() throws Exception {
        prepareIndex(1);
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        for (int i = 0; i < 10; i++) {
            bulkRequestBuilder.add(client().prepareIndex("idx").setSource("{}", MediaTypeRegistry.JSON));
        }

        BulkResponse bulkResponse = bulkRequestBuilder.get();
        bulkRequestBuilder = client().prepareBulk();
        for (BulkItemResponse item : bulkResponse) {
            assertThat(item.isFailed(), equalTo(false));
            assertShardInfo(item.getResponse());
            bulkRequestBuilder.add(client().prepareDelete("idx", item.getId()));
        }

        bulkResponse = bulkRequestBuilder.get();
        for (BulkItemResponse item : bulkResponse) {
            assertThat(item.isFailed(), equalTo(false));
            assertShardInfo(item.getResponse());
        }
    }

    public void testBulkWithUpdateItems() throws Exception {
        prepareIndex(1);
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        for (int i = 0; i < 10; i++) {
            bulkRequestBuilder.add(
                client().prepareUpdate("idx", Integer.toString(i)).setDoc("{}", MediaTypeRegistry.JSON).setDocAsUpsert(true)
            );
        }

        BulkResponse bulkResponse = bulkRequestBuilder.get();
        for (BulkItemResponse item : bulkResponse) {
            assertThat(item.getFailure(), nullValue());
            assertThat(item.isFailed(), equalTo(false));
            assertShardInfo(item.getResponse());
        }
    }

    private void prepareIndex(int numberOfPrimaryShards) throws Exception {
        prepareIndex(numberOfPrimaryShards, false);
    }

    private void prepareIndex(int numberOfPrimaryShards, boolean routingRequired) throws Exception {
        numNodes = cluster().numDataNodes();
        logger.info("Number of nodes: {}", numNodes);
        int maxNumberOfCopies = (numNodes * 2) - 1;
        numCopies = randomIntBetween(numNodes, maxNumberOfCopies);
        logger.info("Number of copies: {}", numCopies);

        assertAcked(
            prepareCreate("idx").setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfPrimaryShards)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numCopies - 1)
            ).setMapping("_routing", "required=" + routingRequired).get()
        );
        for (int i = 0; i < numberOfPrimaryShards; i++) {
            ensureActiveShardCopies(i, numNodes);
        }
    }

    private void assertShardInfo(ReplicationResponse response) {
        assertShardInfo(response, numCopies, numNodes);
    }

    private void assertShardInfo(ReplicationResponse response, int expectedTotal, int expectedSuccessful) {
        assertThat(response.getShardInfo().getTotal(), greaterThanOrEqualTo(expectedTotal));
        assertThat(response.getShardInfo().getSuccessful(), greaterThanOrEqualTo(expectedSuccessful));
    }

    private void ensureActiveShardCopies(final int shardId, final int copyCount) throws Exception {
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            assertThat(state.routingTable().index("idx"), not(nullValue()));
            assertThat(state.routingTable().index("idx").shard(shardId), not(nullValue()));
            assertThat(state.routingTable().index("idx").shard(shardId).activeShards().size(), equalTo(copyCount));

            ClusterHealthResponse healthResponse = client().admin().cluster().prepareHealth("idx").setWaitForNoRelocatingShards(true).get();
            assertThat(healthResponse.isTimedOut(), equalTo(false));

            RecoveryResponse recoveryResponse = client().admin().indices().prepareRecoveries("idx").setActiveOnly(true).get();
            assertThat(recoveryResponse.shardRecoveryStates().get("idx").size(), equalTo(0));
        });
    }
}
