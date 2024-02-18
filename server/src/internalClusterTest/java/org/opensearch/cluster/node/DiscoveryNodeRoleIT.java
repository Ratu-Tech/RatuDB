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

package org.opensearch.cluster.node;

import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.hamcrest.Matcher;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.opensearch.test.NodeRoles.addRoles;
import static org.opensearch.test.NodeRoles.onlyRole;
import static org.opensearch.test.NodeRoles.removeRoles;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class DiscoveryNodeRoleIT extends OpenSearchIntegTestCase {

    public static class AdditionalRolePlugin extends Plugin {

        public AdditionalRolePlugin() {

        }

        static final Setting<Boolean> NODE_ADDITIONAL_SETTING = Setting.boolSetting(
            "node.additional",
            true,
            Property.Deprecated,
            Property.NodeScope
        );

        static DiscoveryNodeRole ADDITIONAL_ROLE = new DiscoveryNodeRole("additional", "a") {

            @Override
            public Setting<Boolean> legacySetting() {
                return NODE_ADDITIONAL_SETTING;
            }

        };

        @Override
        public Set<DiscoveryNodeRole> getRoles() {
            return Collections.singleton(ADDITIONAL_ROLE);
        }

        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(NODE_ADDITIONAL_SETTING);
        }

    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(AdditionalRolePlugin.class);
    }

    public void testDefaultHasAdditionalRole() {
        runTestNodeHasAdditionalRole(Settings.EMPTY);
    }

    public void testExplicitlyHasAdditionalRoleUsingLegacySetting() {
        runTestNodeHasAdditionalRole(Settings.builder().put(AdditionalRolePlugin.NODE_ADDITIONAL_SETTING.getKey(), true).build());
    }

    public void testExplicitlyHasAdditionalRoles() {
        runTestNodeHasAdditionalRole(addRoles(Collections.singleton(AdditionalRolePlugin.ADDITIONAL_ROLE)));
    }

    public void testDoesNotHaveAdditionalRoleUsingLegacySetting() {
        runTestNodeHasAdditionalRole(Settings.builder().put(AdditionalRolePlugin.NODE_ADDITIONAL_SETTING.getKey(), false).build());
    }

    public void testExplicitlyDoesNotHaveAdditionalRole() {
        runTestNodeHasAdditionalRole(removeRoles(Collections.singleton(AdditionalRolePlugin.ADDITIONAL_ROLE)));
    }

    private void runTestNodeHasAdditionalRole(final Settings settings) {
        final String name = internalCluster().startNode(settings);
        final NodesInfoResponse response = client().admin().cluster().prepareNodesInfo(name).get();
        assertThat(response.getNodes(), hasSize(1));
        final Matcher<Iterable<? super DiscoveryNodeRole>> matcher;
        if (DiscoveryNode.hasRole(settings, AdditionalRolePlugin.ADDITIONAL_ROLE)) {
            matcher = hasItem(AdditionalRolePlugin.ADDITIONAL_ROLE);
        } else {
            matcher = not(hasItem(AdditionalRolePlugin.ADDITIONAL_ROLE));
        }
        assertThat(response.getNodes().get(0).getNode().getRoles(), matcher);
    }

    public void testStartNodeWithClusterManagerRoleAndMasterSetting() {
        final Settings settings = Settings.builder()
            .put("node.master", randomBoolean())
            .put(onlyRole(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE))
            .build();

        final IllegalArgumentException e1 = expectThrows(
            IllegalArgumentException.class,
            () -> DiscoveryNode.getRolesFromSettings(settings)
        );
        assertThat(e1.getMessage(), startsWith("can not explicitly configure node roles and use legacy role setting"));
        final IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class, () -> internalCluster().startNodes(settings));
        assertThat(e2.getMessage(), startsWith("can not explicitly configure node roles and use legacy role setting"));
    }
}
