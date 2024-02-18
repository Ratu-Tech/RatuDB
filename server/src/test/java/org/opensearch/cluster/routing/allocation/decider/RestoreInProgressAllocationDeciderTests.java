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

package org.opensearch.cluster.routing.allocation.decider;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.RestoreInProgress;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.common.UUIDs;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.repositories.IndexId;
import org.opensearch.snapshots.Snapshot;
import org.opensearch.snapshots.SnapshotId;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonList;

/**
 * Test {@link RestoreInProgressAllocationDecider}
 */
public class RestoreInProgressAllocationDeciderTests extends OpenSearchAllocationTestCase {

    public void testCanAllocatePrimary() {
        ClusterState clusterState = createInitialClusterState();
        ShardRouting shard;
        if (randomBoolean()) {
            shard = clusterState.getRoutingTable().shardRoutingTable("test", 0).primaryShard();
            assertEquals(RecoverySource.Type.EMPTY_STORE, shard.recoverySource().getType());
        } else {
            shard = clusterState.getRoutingTable().shardRoutingTable("test", 0).replicaShards().get(0);
            assertEquals(RecoverySource.Type.PEER, shard.recoverySource().getType());
        }

        final Decision decision = executeAllocation(clusterState, shard);
        assertEquals(Decision.Type.YES, decision.type());
        assertEquals("ignored as shard is not being recovered from a snapshot", decision.getExplanation());
    }

    public void testCannotAllocatePrimaryMissingInRestoreInProgress() {
        ClusterState clusterState = createInitialClusterState();
        RoutingTable routingTable = RoutingTable.builder(clusterState.getRoutingTable())
            .addAsRestore(clusterState.getMetadata().index("test"), createSnapshotRecoverySource("_missing"))
            .build();

        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        ShardRouting primary = clusterState.getRoutingTable().shardRoutingTable("test", 0).primaryShard();
        assertEquals(ShardRoutingState.UNASSIGNED, primary.state());
        assertEquals(RecoverySource.Type.SNAPSHOT, primary.recoverySource().getType());

        final Decision decision = executeAllocation(clusterState, primary);
        assertEquals(Decision.Type.NO, decision.type());
        assertEquals(
            "shard has failed to be restored from the snapshot [_repository:_missing/_uuid] because of "
                + "[restore_source[_repository/_missing]] - manually close or delete the index [test] in order to retry to restore "
                + "the snapshot again or use the reroute API to force the allocation of an empty primary shard",
            decision.getExplanation()
        );
    }

    public void testCanAllocatePrimaryExistingInRestoreInProgress() {
        RecoverySource.SnapshotRecoverySource recoverySource = createSnapshotRecoverySource("_existing");

        ClusterState clusterState = createInitialClusterState();
        RoutingTable routingTable = RoutingTable.builder(clusterState.getRoutingTable())
            .addAsRestore(clusterState.getMetadata().index("test"), recoverySource)
            .build();

        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        ShardRouting primary = clusterState.getRoutingTable().shardRoutingTable("test", 0).primaryShard();
        assertEquals(ShardRoutingState.UNASSIGNED, primary.state());
        assertEquals(RecoverySource.Type.SNAPSHOT, primary.recoverySource().getType());

        routingTable = clusterState.routingTable();

        final RestoreInProgress.State shardState;
        if (randomBoolean()) {
            shardState = randomFrom(RestoreInProgress.State.STARTED, RestoreInProgress.State.INIT);
        } else {
            shardState = RestoreInProgress.State.FAILURE;

            UnassignedInfo currentInfo = primary.unassignedInfo();
            UnassignedInfo newInfo = new UnassignedInfo(
                currentInfo.getReason(),
                currentInfo.getMessage(),
                new IOException("i/o failure"),
                currentInfo.getNumFailedAllocations(),
                currentInfo.getUnassignedTimeInNanos(),
                currentInfo.getUnassignedTimeInMillis(),
                currentInfo.isDelayed(),
                currentInfo.getLastAllocationStatus(),
                currentInfo.getFailedNodeIds()
            );
            primary = primary.updateUnassigned(newInfo, primary.recoverySource());

            IndexRoutingTable indexRoutingTable = routingTable.index("test");
            IndexRoutingTable.Builder newIndexRoutingTable = IndexRoutingTable.builder(indexRoutingTable.getIndex());
            for (final var shardEntry : indexRoutingTable.getShards().values()) {
                final IndexShardRoutingTable shardRoutingTable = shardEntry;
                for (ShardRouting shardRouting : shardRoutingTable.getShards()) {
                    if (shardRouting.primary()) {
                        newIndexRoutingTable.addShard(primary);
                    } else {
                        newIndexRoutingTable.addShard(shardRouting);
                    }
                }
            }
            routingTable = RoutingTable.builder(routingTable).add(newIndexRoutingTable).build();
        }

        final Map<ShardId, RestoreInProgress.ShardRestoreStatus> shards = new HashMap<>();
        shards.put(primary.shardId(), new RestoreInProgress.ShardRestoreStatus(clusterState.getNodes().getLocalNodeId(), shardState));

        Snapshot snapshot = recoverySource.snapshot();
        RestoreInProgress.State restoreState = RestoreInProgress.State.STARTED;
        RestoreInProgress.Entry restore = new RestoreInProgress.Entry(
            recoverySource.restoreUUID(),
            snapshot,
            restoreState,
            singletonList("test"),
            shards
        );

        clusterState = ClusterState.builder(clusterState)
            .putCustom(RestoreInProgress.TYPE, new RestoreInProgress.Builder().add(restore).build())
            .routingTable(routingTable)
            .build();

        Decision decision = executeAllocation(clusterState, primary);
        if (shardState == RestoreInProgress.State.FAILURE) {
            assertEquals(Decision.Type.NO, decision.type());
            assertEquals(
                "shard has failed to be restored from the snapshot [_repository:_existing/_uuid] because of "
                    + "[restore_source[_repository/_existing], failure IOException[i/o failure]] - manually close or delete the index "
                    + "[test] in order to retry to restore the snapshot again or use the reroute API to force the allocation of "
                    + "an empty primary shard",
                decision.getExplanation()
            );
        } else {
            assertEquals(Decision.Type.YES, decision.type());
            assertEquals("shard is currently being restored", decision.getExplanation());
        }
    }

    private ClusterState createInitialClusterState() {
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(newNode("cluster-manager", Collections.singleton(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE)))
            .localNodeId("cluster-manager")
            .clusterManagerNodeId("cluster-manager")
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(discoveryNodes)
            .build();

        assertEquals(2, clusterState.getRoutingTable().shardsWithState(ShardRoutingState.UNASSIGNED).size());
        return clusterState;
    }

    private Decision executeAllocation(final ClusterState clusterState, final ShardRouting shardRouting) {
        final AllocationDecider decider = new RestoreInProgressAllocationDecider();
        final RoutingAllocation allocation = new RoutingAllocation(
            new AllocationDeciders(Collections.singleton(decider)),
            clusterState.getRoutingNodes(),
            clusterState,
            null,
            null,
            0L
        );
        allocation.debugDecision(true);

        final Decision decision;
        if (randomBoolean()) {
            decision = decider.canAllocate(shardRouting, allocation);
        } else {
            DiscoveryNode node = clusterState.getNodes().getClusterManagerNode();
            decision = decider.canAllocate(shardRouting, new RoutingNode(node.getId(), node), allocation);
        }
        return decision;
    }

    private RecoverySource.SnapshotRecoverySource createSnapshotRecoverySource(final String snapshotName) {
        Snapshot snapshot = new Snapshot("_repository", new SnapshotId(snapshotName, "_uuid"));
        return new RecoverySource.SnapshotRecoverySource(
            UUIDs.randomBase64UUID(),
            snapshot,
            Version.CURRENT,
            new IndexId("test", UUIDs.randomBase64UUID(random()))
        );
    }
}
