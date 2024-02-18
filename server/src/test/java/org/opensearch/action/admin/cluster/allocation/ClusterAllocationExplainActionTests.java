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

package org.opensearch.action.admin.cluster.allocation;

import org.opensearch.action.support.replication.ClusterStateCreationUtils;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.AllocationDecision;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.ShardAllocationDecision;
import org.opensearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.opensearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.gateway.TestGatewayAllocator;

import java.time.Instant;
import java.util.Collections;
import java.util.Locale;

import static org.opensearch.action.admin.cluster.allocation.TransportClusterAllocationExplainAction.findShardToExplain;

/**
 * Tests for the {@link TransportClusterAllocationExplainAction} class.
 */
public class ClusterAllocationExplainActionTests extends OpenSearchTestCase {

    private static final AllocationDeciders NOOP_DECIDERS = new AllocationDeciders(Collections.emptyList());

    public void testInitializingOrRelocatingShardExplanation() throws Exception {
        ShardRoutingState shardRoutingState = randomFrom(ShardRoutingState.INITIALIZING, ShardRoutingState.RELOCATING);
        ClusterState clusterState = ClusterStateCreationUtils.state("idx", randomBoolean(), shardRoutingState);
        ShardRouting shard = clusterState.getRoutingTable().index("idx").shard(0).primaryShard();
        RoutingAllocation allocation = new RoutingAllocation(
            new AllocationDeciders(Collections.emptyList()),
            clusterState.getRoutingNodes(),
            clusterState,
            null,
            null,
            System.nanoTime()
        );
        ClusterAllocationExplanation cae = TransportClusterAllocationExplainAction.explainShard(
            shard,
            allocation,
            null,
            randomBoolean(),
            new AllocationService(null, new TestGatewayAllocator(), new ShardsAllocator() {
                @Override
                public void allocate(RoutingAllocation allocation) {
                    // no-op
                }

                @Override
                public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                    if (shard.initializing() || shard.relocating()) {
                        return ShardAllocationDecision.NOT_TAKEN;
                    } else {
                        throw new UnsupportedOperationException("cannot explain");
                    }
                }
            }, null, null)
        );

        assertEquals(shard.currentNodeId(), cae.getCurrentNode().getId());
        assertFalse(cae.getShardAllocationDecision().isDecisionTaken());
        assertFalse(cae.getShardAllocationDecision().getAllocateDecision().isDecisionTaken());
        assertFalse(cae.getShardAllocationDecision().getMoveDecision().isDecisionTaken());
        XContentBuilder builder = XContentFactory.jsonBuilder();
        cae.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String explanation;
        if (shardRoutingState == ShardRoutingState.RELOCATING) {
            explanation = "the shard is in the process of relocating from node [] to node [], wait until " + "relocation has completed";
        } else {
            explanation = "the shard is in the process of initializing on node [], " + "wait until initialization has completed";
        }
        assertEquals(
            "{\"index\":\"idx\",\"shard\":0,\"primary\":true,\"current_state\":\""
                + shardRoutingState.toString().toLowerCase(Locale.ROOT)
                + "\""
                + (shard.unassignedInfo() != null
                    ? ",\"unassigned_info\":{"
                        + "\"reason\":\""
                        + shard.unassignedInfo().getReason()
                        + "\","
                        + "\"at\":\""
                        + UnassignedInfo.DATE_TIME_FORMATTER.format(
                            Instant.ofEpochMilli(shard.unassignedInfo().getUnassignedTimeInMillis())
                        )
                        + "\","
                        + "\"last_allocation_status\":\""
                        + AllocationDecision.fromAllocationStatus(shard.unassignedInfo().getLastAllocationStatus())
                        + "\"}"
                    : "")
                + ",\"current_node\":"
                + "{\"id\":\""
                + cae.getCurrentNode().getId()
                + "\",\"name\":\""
                + cae.getCurrentNode().getName()
                + "\",\"transport_address\":\""
                + cae.getCurrentNode().getAddress()
                + "\"},\"explanation\":\""
                + explanation
                + "\"}",
            builder.toString()
        );
    }

    public void testFindAnyUnassignedShardToExplain() {
        // find unassigned primary
        ClusterState clusterState = ClusterStateCreationUtils.state("idx", randomBoolean(), ShardRoutingState.UNASSIGNED);
        ClusterAllocationExplainRequest request = new ClusterAllocationExplainRequest();
        ShardRouting shard = findShardToExplain(request, routingAllocation(clusterState));
        assertEquals(clusterState.getRoutingTable().index("idx").shard(0).primaryShard(), shard);

        // find unassigned replica
        clusterState = ClusterStateCreationUtils.state("idx", randomBoolean(), ShardRoutingState.STARTED, ShardRoutingState.UNASSIGNED);
        request = new ClusterAllocationExplainRequest();
        shard = findShardToExplain(request, routingAllocation(clusterState));
        assertEquals(clusterState.getRoutingTable().index("idx").shard(0).replicaShards().get(0), shard);

        // no unassigned shard to explain
        final ClusterState allStartedClusterState = ClusterStateCreationUtils.state(
            "idx",
            randomBoolean(),
            ShardRoutingState.STARTED,
            ShardRoutingState.STARTED
        );
        final ClusterAllocationExplainRequest anyUnassignedShardsRequest = new ClusterAllocationExplainRequest();
        expectThrows(
            IllegalArgumentException.class,
            () -> findShardToExplain(anyUnassignedShardsRequest, routingAllocation(allStartedClusterState))
        );
    }

    public void testFindPrimaryShardToExplain() {
        ClusterState clusterState = ClusterStateCreationUtils.state("idx", randomBoolean(), randomFrom(ShardRoutingState.values()));
        ClusterAllocationExplainRequest request = new ClusterAllocationExplainRequest("idx", 0, true, null);
        ShardRouting shard = findShardToExplain(request, routingAllocation(clusterState));
        assertEquals(clusterState.getRoutingTable().index("idx").shard(0).primaryShard(), shard);
    }

    public void testFindAnyReplicaToExplain() {
        // prefer unassigned replicas to started replicas
        ClusterState clusterState = ClusterStateCreationUtils.state(
            "idx",
            randomBoolean(),
            ShardRoutingState.STARTED,
            ShardRoutingState.STARTED,
            ShardRoutingState.UNASSIGNED
        );
        ClusterAllocationExplainRequest request = new ClusterAllocationExplainRequest("idx", 0, false, null);
        ShardRouting shard = findShardToExplain(request, routingAllocation(clusterState));
        assertEquals(
            clusterState.getRoutingTable()
                .index("idx")
                .shard(0)
                .replicaShards()
                .stream()
                .filter(ShardRouting::unassigned)
                .findFirst()
                .get(),
            shard
        );

        // prefer started replicas to initializing/relocating replicas
        clusterState = ClusterStateCreationUtils.state(
            "idx",
            randomBoolean(),
            ShardRoutingState.STARTED,
            randomFrom(ShardRoutingState.RELOCATING, ShardRoutingState.INITIALIZING),
            ShardRoutingState.STARTED
        );
        request = new ClusterAllocationExplainRequest("idx", 0, false, null);
        shard = findShardToExplain(request, routingAllocation(clusterState));
        assertEquals(
            clusterState.getRoutingTable().index("idx").shard(0).replicaShards().stream().filter(ShardRouting::started).findFirst().get(),
            shard
        );
    }

    public void testFindShardAssignedToNode() {
        // find shard with given node
        final boolean primary = randomBoolean();
        ShardRoutingState[] replicaStates = new ShardRoutingState[0];
        if (primary == false) {
            replicaStates = new ShardRoutingState[] { ShardRoutingState.STARTED };
        }
        ClusterState clusterState = ClusterStateCreationUtils.state("idx", randomBoolean(), ShardRoutingState.STARTED, replicaStates);
        ShardRouting shardToExplain = primary
            ? clusterState.getRoutingTable().index("idx").shard(0).primaryShard()
            : clusterState.getRoutingTable().index("idx").shard(0).replicaShards().get(0);
        ClusterAllocationExplainRequest request = new ClusterAllocationExplainRequest("idx", 0, primary, shardToExplain.currentNodeId());
        RoutingAllocation allocation = routingAllocation(clusterState);
        ShardRouting foundShard = findShardToExplain(request, allocation);
        assertEquals(shardToExplain, foundShard);

        // shard is not assigned to given node
        String explainNode = null;
        for (RoutingNode routingNode : clusterState.getRoutingNodes()) {
            if (routingNode.nodeId().equals(shardToExplain.currentNodeId()) == false) {
                explainNode = routingNode.nodeId();
                break;
            }
        }
        final ClusterAllocationExplainRequest failingRequest = new ClusterAllocationExplainRequest("idx", 0, primary, explainNode);
        expectThrows(IllegalArgumentException.class, () -> findShardToExplain(failingRequest, allocation));
    }

    private static RoutingAllocation routingAllocation(ClusterState clusterState) {
        return new RoutingAllocation(NOOP_DECIDERS, clusterState.getRoutingNodes(), clusterState, null, null, System.nanoTime());
    }
}
