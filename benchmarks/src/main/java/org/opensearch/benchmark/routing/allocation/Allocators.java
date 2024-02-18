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

package org.opensearch.benchmark.routing.allocation;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.EmptyClusterInfoService;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.FailedShard;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.cluster.routing.allocation.decider.AllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.gateway.GatewayAllocator;
import org.opensearch.snapshots.EmptySnapshotsInfoService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public final class Allocators {
    private static class NoopGatewayAllocator extends GatewayAllocator {
        public static final NoopGatewayAllocator INSTANCE = new NoopGatewayAllocator();

        @Override
        public void applyStartedShards(List<ShardRouting> startedShards, RoutingAllocation allocation) {
            // noop
        }

        @Override
        public void applyFailedShards(List<FailedShard> failedShards, RoutingAllocation allocation) {
            // noop
        }

        @Override
        public void allocateUnassigned(
            ShardRouting shardRouting,
            RoutingAllocation allocation,
            UnassignedAllocationHandler unassignedAllocationHandler
        ) {
            // noop
        }
    }

    private Allocators() {
        throw new AssertionError("Do not instantiate");
    }

    public static AllocationService createAllocationService(Settings settings) {
        return createAllocationService(settings, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
    }

    public static AllocationService createAllocationService(Settings settings, ClusterSettings clusterSettings) {
        return new AllocationService(
            defaultAllocationDeciders(settings, clusterSettings),
            NoopGatewayAllocator.INSTANCE,
            new BalancedShardsAllocator(settings),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE
        );
    }

    public static AllocationDeciders defaultAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
        Collection<AllocationDecider> deciders = ClusterModule.createAllocationDeciders(settings, clusterSettings, Collections.emptyList());
        return new AllocationDeciders(deciders);
    }

    private static final AtomicInteger portGenerator = new AtomicInteger();

    public static DiscoveryNode newNode(String nodeId, Map<String, String> attributes) {
        return new DiscoveryNode(
            "",
            nodeId,
            new TransportAddress(TransportAddress.META_ADDRESS, portGenerator.incrementAndGet()),
            attributes,
            Sets.newHashSet(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
    }
}
