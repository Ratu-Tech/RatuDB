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

package org.opensearch.gateway;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.FailedNodeException;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * The Gateway
 *
 * @opensearch.internal
 */
public class Gateway {

    private static final Logger logger = LogManager.getLogger(Gateway.class);

    private final ClusterService clusterService;

    private final TransportNodesListGatewayMetaState listGatewayMetaState;

    public Gateway(
        final Settings settings,
        final ClusterService clusterService,
        final TransportNodesListGatewayMetaState listGatewayMetaState
    ) {
        this.clusterService = clusterService;
        this.listGatewayMetaState = listGatewayMetaState;
    }

    public void performStateRecovery(final GatewayStateRecoveredListener listener) throws GatewayException {
        final String[] nodesIds = clusterService.state().nodes().getClusterManagerNodes().keySet().toArray(new String[0]);
        logger.trace("performing state recovery from {}", Arrays.toString(nodesIds));
        final TransportNodesListGatewayMetaState.NodesGatewayMetaState nodesState = listGatewayMetaState.list(nodesIds, null).actionGet();

        final int requiredAllocation = 1;

        if (nodesState.hasFailures()) {
            for (final FailedNodeException failedNodeException : nodesState.failures()) {
                logger.warn("failed to fetch state from node", failedNodeException);
            }
        }

        final Map<Index, Float> indices = new HashMap<>();
        Metadata electedGlobalState = null;
        int found = 0;
        for (final TransportNodesListGatewayMetaState.NodeGatewayMetaState nodeState : nodesState.getNodes()) {
            if (nodeState.metadata() == null) {
                continue;
            }
            found++;
            if (electedGlobalState == null) {
                electedGlobalState = nodeState.metadata();
            } else if (nodeState.metadata().version() > electedGlobalState.version()) {
                electedGlobalState = nodeState.metadata();
            }
            for (final IndexMetadata cursor : nodeState.metadata().indices().values()) {
                indices.merge(cursor.getIndex(), 1f, Float::sum);
            }
        }
        if (found < requiredAllocation) {
            listener.onFailure("found [" + found + "] metadata states, required [" + requiredAllocation + "]");
            return;
        }
        // update the global state, and clean the indices, we elect them in the next phase
        final Metadata.Builder metadataBuilder = Metadata.builder(electedGlobalState).removeAllIndices();

        assert !indices.containsKey(null);
        final Object[] keys = indices.keySet().toArray();
        for (int i = 0; i < keys.length; i++) {
            if (keys[i] != null) {
                final Index index = (Index) keys[i];
                IndexMetadata electedIndexMetadata = null;
                int indexMetadataCount = 0;
                for (final TransportNodesListGatewayMetaState.NodeGatewayMetaState nodeState : nodesState.getNodes()) {
                    if (nodeState.metadata() == null) {
                        continue;
                    }
                    final IndexMetadata indexMetadata = nodeState.metadata().index(index);
                    if (indexMetadata == null) {
                        continue;
                    }
                    if (electedIndexMetadata == null) {
                        electedIndexMetadata = indexMetadata;
                    } else if (indexMetadata.getVersion() > electedIndexMetadata.getVersion()) {
                        electedIndexMetadata = indexMetadata;
                    }
                    indexMetadataCount++;
                }
                if (electedIndexMetadata != null) {
                    if (indexMetadataCount < requiredAllocation) {
                        logger.debug("[{}] found [{}], required [{}], not adding", index, indexMetadataCount, requiredAllocation);
                    } // TODO if this logging statement is correct then we are missing an else here

                    metadataBuilder.put(electedIndexMetadata, false);
                }
            }
        }
        ClusterState recoveredState = Function.<ClusterState>identity()
            .andThen(state -> ClusterStateUpdaters.upgradeAndArchiveUnknownOrInvalidSettings(state, clusterService.getClusterSettings()))
            .apply(ClusterState.builder(clusterService.getClusterName()).metadata(metadataBuilder).build());

        listener.onSuccess(recoveredState);
    }

    /**
     * The lister for state recovered.
     *
     * @opensearch.internal
     */
    public interface GatewayStateRecoveredListener {
        void onSuccess(ClusterState build);

        void onFailure(String s);
    }
}
