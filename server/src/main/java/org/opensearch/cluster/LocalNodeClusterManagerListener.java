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

/**
 * Enables listening to cluster-manager changes events of the local node (when the local node becomes the cluster-manager, and when the local
 * node cease being a cluster-manager).
 *
 * @opensearch.internal
 */
public interface LocalNodeClusterManagerListener extends ClusterStateListener {

    /**
     * Called when local node is elected to be the cluster-manager
     */
    void onClusterManager();

    /**
     * Called when the local node used to be the cluster-manager, a new cluster-manager was elected and it's no longer the local node.
     */
    void offClusterManager();

    @Override
    default void clusterChanged(ClusterChangedEvent event) {
        final boolean wasClusterManager = event.previousState().nodes().isLocalNodeElectedClusterManager();
        final boolean isClusterManager = event.localNodeClusterManager();
        if (wasClusterManager == false && isClusterManager) {
            onClusterManager();
        } else if (wasClusterManager && isClusterManager == false) {
            offClusterManager();
        }
    }
}
