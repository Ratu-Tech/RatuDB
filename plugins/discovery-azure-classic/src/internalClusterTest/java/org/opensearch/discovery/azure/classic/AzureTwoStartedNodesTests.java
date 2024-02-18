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

package org.opensearch.discovery.azure.classic;

import org.opensearch.cloud.azure.classic.AbstractAzureComputeServiceTestCase;
import org.opensearch.cloud.azure.classic.management.AzureComputeService.Discovery;
import org.opensearch.cloud.azure.classic.management.AzureComputeService.Management;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchIntegTestCase;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class AzureTwoStartedNodesTests extends AbstractAzureComputeServiceTestCase {

    public void testTwoNodesShouldRunUsingPrivateOrPublicIp() {
        final String hostType = randomFrom(AzureSeedHostsProvider.HostType.values()).getType();
        logger.info("--> using azure host type " + hostType);

        final Settings settings = Settings.builder()
            .put(Management.SERVICE_NAME_SETTING.getKey(), "dummy")
            .put(Discovery.HOST_TYPE_SETTING.getKey(), hostType)
            .build();

        logger.info("--> start first node");
        final String node1 = internalCluster().startNode(settings);
        registerAzureNode(node1);
        assertNotNull(
            client().admin().cluster().prepareState().setClusterManagerNodeTimeout("1s").get().getState().nodes().getClusterManagerNodeId()
        );

        logger.info("--> start another node");
        final String node2 = internalCluster().startNode(settings);
        registerAzureNode(node2);
        assertNotNull(
            client().admin().cluster().prepareState().setClusterManagerNodeTimeout("1s").get().getState().nodes().getClusterManagerNodeId()
        );

        // We expect having 2 nodes as part of the cluster, let's test that
        assertNumberOfNodes(2);
    }
}
