/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.decider;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingPool;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;

/**
 * {@link TargetPoolAllocationDecider} ensures that the different shard types are assigned to the nodes with
 * appropriate capabilities. The node pools with respective capabilities are defined within {@link RoutingPool}.
 *
 * @opensearch.internal
 */
public class TargetPoolAllocationDecider extends AllocationDecider {
    private static final Logger logger = LogManager.getLogger(TargetPoolAllocationDecider.class);

    public static final String NAME = "target_pool";

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        RoutingPool shardPool = RoutingPool.getShardPool(shardRouting, allocation);
        RoutingPool targetNodePool = RoutingPool.getNodePool(node);
        if (RoutingPool.REMOTE_CAPABLE.equals(shardPool) && RoutingPool.LOCAL_ONLY.equals(targetNodePool)) {
            logger.debug(
                "Shard: [{}] has target pool: [{}]. Cannot allocate on node: [{}] with target pool: [{}]",
                shardRouting,
                shardPool,
                node.node(),
                targetNodePool
            );
            return allocation.decision(
                Decision.NO,
                NAME,
                "Routing pools are incompatible. Shard pool: [%s], Node Pool: [%s]",
                shardPool,
                targetNodePool
            );
        } else if (RoutingPool.LOCAL_ONLY.equals(shardPool)
            && RoutingPool.REMOTE_CAPABLE.equals(targetNodePool)
            && !node.node().getRoles().contains(DiscoveryNodeRole.DATA_ROLE)) {
                logger.debug(
                    "Shard: [{}] has target pool: [{}]. Cannot allocate on node: [{}] without the [{}] node role",
                    shardRouting,
                    shardPool,
                    node.node(),
                    DiscoveryNodeRole.DATA_ROLE
                );
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "Routing pools are incompatible. Shard pool: [{}], Node Pool: [{}] without [{}] role",
                    shardPool,
                    targetNodePool,
                    DiscoveryNodeRole.DATA_ROLE
                );
            }
        return allocation.decision(
            Decision.YES,
            NAME,
            "Routing pools are compatible. Shard pool: [%s], Node Pool: [%s]",
            shardPool,
            targetNodePool
        );
    }

    @Override
    public Decision canAllocate(IndexMetadata indexMetadata, RoutingNode node, RoutingAllocation allocation) {
        return canAllocateInTargetPool(indexMetadata, node.node(), allocation);
    }

    @Override
    public Decision canForceAllocatePrimary(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        logger.debug("Evaluating force allocation for primary shard.");
        return canAllocate(shardRouting, node, allocation);
    }

    public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
        logger.debug("Evaluating node: {} for autoExpandReplica eligibility of index: {}", node, indexMetadata.getIndex());
        return canAllocateInTargetPool(indexMetadata, node, allocation);
    }

    private Decision canAllocateInTargetPool(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
        RoutingPool indexPool = RoutingPool.getIndexPool(indexMetadata);
        RoutingPool targetNodePool = RoutingPool.getNodePool(node);
        if (RoutingPool.REMOTE_CAPABLE.equals(indexPool) && RoutingPool.LOCAL_ONLY.equals(targetNodePool)) {
            logger.debug(
                "Index: [{}] has target pool: [{}]. Cannot allocate on node: [{}] with target pool: [{}]",
                indexMetadata.getIndex().getName(),
                indexPool,
                node,
                targetNodePool
            );
            return allocation.decision(
                Decision.NO,
                NAME,
                "Routing pools are incompatible. Index pool: [%s], Node Pool: [%s]",
                indexPool,
                targetNodePool
            );
        } else if (RoutingPool.LOCAL_ONLY.equals(indexPool)
            && RoutingPool.REMOTE_CAPABLE.equals(targetNodePool)
            && !node.getRoles().contains(DiscoveryNodeRole.DATA_ROLE)) {
                logger.debug(
                    "Index: [{}] has target pool: [{}]. Cannot allocate on node: [{}] without the [{}] node role",
                    indexMetadata.getIndex().getName(),
                    indexPool,
                    node,
                    DiscoveryNodeRole.DATA_ROLE
                );
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "Routing pools are incompatible. Index pool: [{}], Node Pool: [{}] without [{}] role",
                    indexPool,
                    targetNodePool,
                    DiscoveryNodeRole.DATA_ROLE
                );
            }
        return allocation.decision(
            Decision.YES,
            NAME,
            "Routing pools are compatible. Index pool: [%s], Node Pool: [%s]",
            indexPool,
            targetNodePool
        );
    }

}
