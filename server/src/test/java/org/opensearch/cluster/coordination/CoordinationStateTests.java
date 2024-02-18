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

package org.opensearch.cluster.coordination;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.opensearch.cluster.coordination.CoordinationState.PersistedState;
import org.opensearch.cluster.coordination.PersistedStateRegistry.PersistedStateType;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.Assertions;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.gateway.GatewayMetaState.RemotePersistedState;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.test.EqualsHashCodeTestUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class CoordinationStateTests extends OpenSearchTestCase {

    private DiscoveryNode node1;
    private DiscoveryNode node2;
    private DiscoveryNode node3;

    private ClusterState initialStateNode1;

    private PersistedState ps1;
    private PersistedStateRegistry psr1;

    private CoordinationState cs1;
    private CoordinationState cs2;
    private CoordinationState cs3;

    @Before
    public void setupNodes() {
        node1 = createNode("node1");
        node2 = createNode("node2");
        node3 = createNode("node3");

        initialStateNode1 = clusterState(0L, 0L, node1, VotingConfiguration.EMPTY_CONFIG, VotingConfiguration.EMPTY_CONFIG, 42L);
        ClusterState initialStateNode2 = clusterState(
            0L,
            0L,
            node2,
            VotingConfiguration.EMPTY_CONFIG,
            VotingConfiguration.EMPTY_CONFIG,
            42L
        );
        ClusterState initialStateNode3 = clusterState(
            0L,
            0L,
            node3,
            VotingConfiguration.EMPTY_CONFIG,
            VotingConfiguration.EMPTY_CONFIG,
            42L
        );

        ps1 = new InMemoryPersistedState(0L, initialStateNode1);
        psr1 = persistedStateRegistry();
        psr1.addPersistedState(PersistedStateType.LOCAL, ps1);

        cs1 = createCoordinationState(psr1, node1, Settings.EMPTY);
        cs2 = createCoordinationState(createPersistedStateRegistry(initialStateNode2), node2, Settings.EMPTY);
        cs3 = createCoordinationState(createPersistedStateRegistry(initialStateNode3), node3, Settings.EMPTY);
    }

    public static DiscoveryNode createNode(String id) {
        final TransportAddress address = buildNewFakeTransportAddress();
        return new DiscoveryNode(
            "",
            id,
            UUIDs.randomBase64UUID(random()), // generated deterministically for repeatable tests
            address.address().getHostString(),
            address.getAddress(),
            address,
            Collections.emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
    }

    public void testSetInitialState() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        assertTrue(state1.getLastAcceptedConfiguration().hasQuorum(Collections.singleton(node1.getId())));
        assertTrue(state1.getLastCommittedConfiguration().hasQuorum(Collections.singleton(node1.getId())));
        cs1.setInitialState(state1);
        assertThat(cs1.getLastAcceptedState(), equalTo(state1));
    }

    public void testSetInitialStateWhenAlreadySet() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        assertTrue(state1.getLastAcceptedConfiguration().hasQuorum(Collections.singleton(node1.getId())));
        assertTrue(state1.getLastCommittedConfiguration().hasQuorum(Collections.singleton(node1.getId())));
        cs1.setInitialState(state1);
        assertThat(
            expectThrows(CoordinationStateRejectedException.class, () -> cs1.setInitialState(state1)).getMessage(),
            containsString("initial state already set")
        );
    }

    public void testStartJoinBeforeBootstrap() {
        assertThat(cs1.getCurrentTerm(), equalTo(0L));
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(randomFrom(node1, node2), randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertThat(v1.getTargetNode(), equalTo(startJoinRequest1.getSourceNode()));
        assertThat(v1.getSourceNode(), equalTo(node1));
        assertThat(v1.getTerm(), equalTo(startJoinRequest1.getTerm()));
        assertThat(v1.getLastAcceptedTerm(), equalTo(initialStateNode1.term()));
        assertThat(v1.getLastAcceptedVersion(), equalTo(initialStateNode1.version()));
        assertThat(cs1.getCurrentTerm(), equalTo(startJoinRequest1.getTerm()));

        StartJoinRequest startJoinRequest2 = new StartJoinRequest(
            randomFrom(node1, node2),
            randomLongBetween(0, startJoinRequest1.getTerm())
        );
        expectThrows(CoordinationStateRejectedException.class, () -> cs1.handleStartJoin(startJoinRequest2));
    }

    public void testStartJoinAfterBootstrap() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        assertTrue(state1.getLastAcceptedConfiguration().hasQuorum(Collections.singleton(node1.getId())));
        assertTrue(state1.getLastCommittedConfiguration().hasQuorum(Collections.singleton(node1.getId())));
        cs1.setInitialState(state1);

        StartJoinRequest startJoinRequest1 = new StartJoinRequest(randomFrom(node1, node2), randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertThat(v1.getTargetNode(), equalTo(startJoinRequest1.getSourceNode()));
        assertThat(v1.getSourceNode(), equalTo(node1));
        assertThat(v1.getTerm(), equalTo(startJoinRequest1.getTerm()));
        assertThat(v1.getLastAcceptedTerm(), equalTo(state1.term()));
        assertThat(v1.getLastAcceptedVersion(), equalTo(state1.version()));
        assertThat(cs1.getCurrentTerm(), equalTo(startJoinRequest1.getTerm()));

        StartJoinRequest startJoinRequest2 = new StartJoinRequest(
            randomFrom(node1, node2),
            randomLongBetween(0, startJoinRequest1.getTerm())
        );
        assertThat(
            expectThrows(CoordinationStateRejectedException.class, () -> cs1.handleStartJoin(startJoinRequest2)).getMessage(),
            containsString("not greater than current term")
        );
        StartJoinRequest startJoinRequest3 = new StartJoinRequest(randomFrom(node1, node2), startJoinRequest1.getTerm());
        assertThat(
            expectThrows(CoordinationStateRejectedException.class, () -> cs1.handleStartJoin(startJoinRequest3)).getMessage(),
            containsString("not greater than current term")
        );
    }

    public void testJoinBeforeBootstrap() {
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertThat(
            expectThrows(CoordinationStateRejectedException.class, () -> cs1.handleJoin(v1)).getMessage(),
            containsString("this node has not received its initial configuration yet")
        );
    }

    public void testJoinWithNoStartJoinAfterReboot() {
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        cs1 = createCoordinationState(psr1, node1, Settings.EMPTY);
        assertThat(
            expectThrows(CoordinationStateRejectedException.class, () -> cs1.handleJoin(v1)).getMessage(),
            containsString("ignored join as term has not been incremented yet after reboot")
        );
    }

    public void testJoinWithWrongTarget() {
        assumeTrue("test only works with assertions enabled", Assertions.ENABLED);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node2, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertThat(expectThrows(AssertionError.class, () -> cs1.handleJoin(v1)).getMessage(), containsString("wrong node"));
    }

    public void testJoinWithBadCurrentTerm() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);

        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node2, randomLongBetween(1, 5));
        cs1.handleStartJoin(startJoinRequest1);
        Join badJoin = new Join(
            randomFrom(node1, node2),
            node1,
            randomLongBetween(0, startJoinRequest1.getTerm() - 1),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
        assertThat(
            expectThrows(CoordinationStateRejectedException.class, () -> cs1.handleJoin(badJoin)).getMessage(),
            containsString("does not match current term")
        );
    }

    public void testJoinWithHigherAcceptedTerm() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);

        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node2, randomLongBetween(1, 5));
        cs1.handleStartJoin(startJoinRequest1);
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(2, 20), node1, initialConfig, initialConfig, 42L);
        cs1.handlePublishRequest(new PublishRequest(state2));
        StartJoinRequest startJoinRequest2 = new StartJoinRequest(node2, randomLongBetween(startJoinRequest1.getTerm() + 1, 10));
        Join v1 = cs1.handleStartJoin(startJoinRequest2);

        Join badJoin = new Join(
            randomFrom(node1, node2),
            node1,
            v1.getTerm(),
            randomLongBetween(state2.term() + 1, 30),
            randomNonNegativeLong()
        );
        assertThat(
            expectThrows(CoordinationStateRejectedException.class, () -> cs1.handleJoin(badJoin)).getMessage(),
            containsString("higher than current last accepted term")
        );
    }

    public void testJoinWithSameAcceptedTermButHigherVersion() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);

        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node2, randomLongBetween(1, 5));
        cs1.handleStartJoin(startJoinRequest1);
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(2, 20), node1, initialConfig, initialConfig, 42L);
        cs1.handlePublishRequest(new PublishRequest(state2));
        StartJoinRequest startJoinRequest2 = new StartJoinRequest(node2, randomLongBetween(startJoinRequest1.getTerm() + 1, 10));
        Join v1 = cs1.handleStartJoin(startJoinRequest2);

        Join badJoin = new Join(randomFrom(node1, node2), node1, v1.getTerm(), state2.term(), randomLongBetween(state2.version() + 1, 30));
        assertThat(
            expectThrows(CoordinationStateRejectedException.class, () -> cs1.handleJoin(badJoin)).getMessage(),
            containsString("higher than current last accepted version")
        );
    }

    public void testJoinWithLowerLastAcceptedTermWinsElection() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);

        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node2, randomLongBetween(1, 5));
        cs1.handleStartJoin(startJoinRequest1);
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(2, 20), node1, initialConfig, initialConfig, 42L);
        cs1.handlePublishRequest(new PublishRequest(state2));
        StartJoinRequest startJoinRequest2 = new StartJoinRequest(node2, randomLongBetween(startJoinRequest1.getTerm() + 1, 10));
        Join v1 = cs1.handleStartJoin(startJoinRequest2);

        Join join = new Join(node1, node1, v1.getTerm(), randomLongBetween(0, state2.term() - 1), randomLongBetween(0, 20));
        assertTrue(cs1.handleJoin(join));
        assertTrue(cs1.electionWon());
        assertTrue(cs1.containsJoinVoteFor(node1));
        assertTrue(cs1.containsJoin(join));
        assertFalse(cs1.containsJoinVoteFor(node2));
        assertEquals(cs1.getLastPublishedVersion(), cs1.getLastAcceptedVersion());
        assertFalse(cs1.handleJoin(join));
    }

    public void testJoinWithSameLastAcceptedTermButLowerOrSameVersionWinsElection() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);

        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node2, randomLongBetween(1, 5));
        cs1.handleStartJoin(startJoinRequest1);
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(2, 20), node1, initialConfig, initialConfig, 42L);
        cs1.handlePublishRequest(new PublishRequest(state2));
        StartJoinRequest startJoinRequest2 = new StartJoinRequest(node2, randomLongBetween(startJoinRequest1.getTerm() + 1, 10));
        Join v1 = cs1.handleStartJoin(startJoinRequest2);

        Join join = new Join(node1, node1, v1.getTerm(), state2.term(), randomLongBetween(0, state2.version()));
        assertTrue(cs1.handleJoin(join));
        assertTrue(cs1.electionWon());
        assertTrue(cs1.containsJoinVoteFor(node1));
        assertFalse(cs1.containsJoinVoteFor(node2));
        assertEquals(cs1.getLastPublishedVersion(), cs1.getLastAcceptedVersion());
        assertFalse(cs1.handleJoin(join));
    }

    public void testJoinDoesNotWinElection() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);

        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node2, randomLongBetween(1, 5));
        cs1.handleStartJoin(startJoinRequest1);
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(2, 20), node1, initialConfig, initialConfig, 42L);
        cs1.handlePublishRequest(new PublishRequest(state2));
        StartJoinRequest startJoinRequest2 = new StartJoinRequest(node2, randomLongBetween(startJoinRequest1.getTerm() + 1, 10));
        Join v1 = cs1.handleStartJoin(startJoinRequest2);

        Join join = new Join(node2, node1, v1.getTerm(), randomLongBetween(0, state2.term()), randomLongBetween(0, state2.version()));
        assertTrue(cs1.handleJoin(join));
        assertFalse(cs1.electionWon());
        assertEquals(cs1.getLastPublishedVersion(), 0L);
        assertFalse(cs1.handleJoin(join));
    }

    public void testJoinDoesNotWinElectionWhenOnlyCommittedConfigQuorum() {
        VotingConfiguration configNode1 = VotingConfiguration.of(node1);
        VotingConfiguration configNode2 = VotingConfiguration.of(node2);
        ClusterState state1 = clusterState(0L, 0L, node1, configNode1, configNode2, 42L);
        cs1.setInitialState(state1);

        StartJoinRequest startJoinRequest = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join join = cs1.handleStartJoin(startJoinRequest);
        assertTrue(cs1.handleJoin(join));
        assertFalse(cs1.electionWon());
        assertEquals(cs1.getLastPublishedVersion(), 0L);
        assertFalse(cs1.handleJoin(join));
    }

    public void testJoinDoesNotWinElectionWhenOnlyLastAcceptedConfigQuorum() {
        VotingConfiguration configNode1 = VotingConfiguration.of(node1);
        VotingConfiguration configNode2 = VotingConfiguration.of(node2);
        ClusterState state1 = clusterState(0L, 0L, node1, configNode2, configNode1, 42L);
        cs1.setInitialState(state1);

        StartJoinRequest startJoinRequest = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join join = cs1.handleStartJoin(startJoinRequest);
        assertTrue(cs1.handleJoin(join));
        assertFalse(cs1.electionWon());
        assertEquals(cs1.getLastPublishedVersion(), 0L);
        assertFalse(cs1.handleJoin(join));
    }

    public void testHandleClientValue() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        Join v2 = cs2.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());
        assertTrue(cs1.containsJoin(v1));
        assertFalse(cs1.containsJoin(v2));
        assertTrue(cs1.handleJoin(v2));
        assertTrue(cs1.containsJoin(v2));

        VotingConfiguration newConfig = VotingConfiguration.of(node2);

        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, newConfig, 42L);
        PublishRequest publishRequest = cs1.handleClientValue(state2);
        assertThat(publishRequest.getAcceptedState(), equalTo(state2));
        assertThat(cs1.getLastPublishedVersion(), equalTo(state2.version()));
        // check that another join does not mess with lastPublishedVersion
        Join v3 = cs3.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v3));
        assertThat(cs1.getLastPublishedVersion(), equalTo(state2.version()));
    }

    public void testHandleClientValueWhenElectionNotWon() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        if (randomBoolean()) {
            cs1.setInitialState(state1);
        }
        assertThat(
            expectThrows(CoordinationStateRejectedException.class, () -> cs1.handleClientValue(state1)).getMessage(),
            containsString("election not won")
        );
    }

    public void testHandleClientValueDuringOngoingPublication() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());

        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, initialConfig, 42L);
        cs1.handleClientValue(state2);

        ClusterState state3 = clusterState(startJoinRequest1.getTerm(), 3L, node1, initialConfig, initialConfig, 42L);
        assertThat(
            expectThrows(CoordinationStateRejectedException.class, () -> cs1.handleClientValue(state3)).getMessage(),
            containsString("cannot start publishing next value before accepting previous one")
        );
    }

    public void testHandleClientValueWithBadTerm() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(3, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());

        long term = randomBoolean()
            ? randomLongBetween(startJoinRequest1.getTerm() + 1, 10)
            : randomLongBetween(0, startJoinRequest1.getTerm() - 1);
        ClusterState state2 = clusterState(term, 2L, node1, initialConfig, initialConfig, 42L);
        assertThat(
            expectThrows(CoordinationStateRejectedException.class, () -> cs1.handleClientValue(state2)).getMessage(),
            containsString("does not match current term")
        );
    }

    public void testHandleClientValueWithOldVersion() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());

        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 0L, node1, initialConfig, initialConfig, 42L);
        assertThat(
            expectThrows(CoordinationStateRejectedException.class, () -> cs1.handleClientValue(state2)).getMessage(),
            containsString("lower or equal to last published version")
        );
    }

    public void testHandleClientValueWithDifferentReconfigurationWhileAlreadyReconfiguring() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        Join v2 = cs2.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());
        assertTrue(cs1.handleJoin(v2));

        VotingConfiguration newConfig1 = VotingConfiguration.of(node2);
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, newConfig1, 42L);
        PublishRequest publishRequest = cs1.handleClientValue(state2);
        cs1.handlePublishRequest(publishRequest);
        VotingConfiguration newConfig2 = VotingConfiguration.of(node3);
        ClusterState state3 = clusterState(startJoinRequest1.getTerm(), 3L, node1, initialConfig, newConfig2, 42L);
        assertThat(
            expectThrows(CoordinationStateRejectedException.class, () -> cs1.handleClientValue(state3)).getMessage(),
            containsString("only allow reconfiguration while not already reconfiguring")
        );
    }

    public void testHandleClientValueWithSameReconfigurationWhileAlreadyReconfiguring() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        Join v2 = cs2.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());
        assertTrue(cs1.handleJoin(v2));

        VotingConfiguration newConfig1 = VotingConfiguration.of(node2);
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, newConfig1, 42L);
        PublishRequest publishRequest = cs1.handleClientValue(state2);
        cs1.handlePublishRequest(publishRequest);
        ClusterState state3 = clusterState(startJoinRequest1.getTerm(), 3L, node1, initialConfig, newConfig1, 42L);
        cs1.handleClientValue(state3);
    }

    public void testHandleClientValueWithIllegalCommittedConfigurationChange() {
        assumeTrue("test only works with assertions enabled", Assertions.ENABLED);
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        Join v2 = cs2.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());
        assertTrue(cs1.handleJoin(v2));

        VotingConfiguration newConfig = VotingConfiguration.of(node2);
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, newConfig, newConfig, 42L);
        assertThat(
            expectThrows(AssertionError.class, () -> cs1.handleClientValue(state2)).getMessage(),
            containsString("last committed configuration should not change")
        );
    }

    public void testHandleClientValueWithConfigurationChangeButNoJoinQuorum() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());

        VotingConfiguration newConfig = VotingConfiguration.of(node2);
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, newConfig, 42L);
        assertThat(
            expectThrows(CoordinationStateRejectedException.class, () -> cs1.handleClientValue(state2)).getMessage(),
            containsString("only allow reconfiguration if joinVotes have quorum for new config")
        );
    }

    public void testHandlePublishRequest() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        if (randomBoolean()) {
            assertTrue(cs1.handleJoin(v1));
            assertTrue(cs1.electionWon());
        }
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(1, 10), node1, initialConfig, initialConfig, 13L);
        PublishResponse publishResponse = cs1.handlePublishRequest(new PublishRequest(state2));
        assertThat(publishResponse.getTerm(), equalTo(state2.term()));
        assertThat(publishResponse.getVersion(), equalTo(state2.version()));
        assertThat(cs1.getLastAcceptedState(), equalTo(state2));
        assertThat(value(cs1.getLastAcceptedState()), equalTo(13L));
        ClusterState state3 = clusterState(
            startJoinRequest1.getTerm(),
            randomLongBetween(state2.getVersion() + 1, 20),
            node1,
            initialConfig,
            initialConfig,
            13L
        );
        cs1.handlePublishRequest(new PublishRequest(state3));
    }

    public void testHandlePublishRequestWithBadTerm() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        if (randomBoolean()) {
            assertTrue(cs1.handleJoin(v1));
            assertTrue(cs1.electionWon());
        }
        long term = randomBoolean()
            ? randomLongBetween(startJoinRequest1.getTerm() + 1, 10)
            : randomLongBetween(0, startJoinRequest1.getTerm() - 1);
        ClusterState state2 = clusterState(term, 2L, node1, initialConfig, initialConfig, 42L);
        assertThat(
            expectThrows(CoordinationStateRejectedException.class, () -> cs1.handlePublishRequest(new PublishRequest(state2))).getMessage(),
            containsString("does not match current term")
        );
    }

    // scenario when handling a publish request from a cluster-manager that we already received a newer state from
    public void testHandlePublishRequestWithSameTermButOlderOrSameVersion() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        if (randomBoolean()) {
            assertTrue(cs1.handleJoin(v1));
            assertTrue(cs1.electionWon());
        }
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(2, 10), node1, initialConfig, initialConfig, 42L);
        cs1.handlePublishRequest(new PublishRequest(state2));
        ClusterState state3 = clusterState(
            startJoinRequest1.getTerm(),
            randomLongBetween(0, state2.version()),
            node1,
            initialConfig,
            initialConfig,
            42L
        );
        assertThat(
            expectThrows(CoordinationStateRejectedException.class, () -> cs1.handlePublishRequest(new PublishRequest(state3))).getMessage(),
            containsString("lower or equal to current version")
        );
    }

    // scenario when handling a publish request from a fresh cluster-manager
    public void testHandlePublishRequestWithTermHigherThanLastAcceptedTerm() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        ClusterState state1 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(2, 10), node1, initialConfig, initialConfig, 42L);
        cs2.handleStartJoin(startJoinRequest1);
        cs2.handlePublishRequest(new PublishRequest(state1));
        StartJoinRequest startJoinRequest2 = new StartJoinRequest(node1, randomLongBetween(startJoinRequest1.getTerm() + 1, 10));
        cs2.handleStartJoin(startJoinRequest2);
        ClusterState state2 = clusterState(startJoinRequest2.getTerm(), randomLongBetween(0, 20), node1, initialConfig, initialConfig, 42L);
        cs2.handlePublishRequest(new PublishRequest(state2));
    }

    public void testHandlePublishResponseWithCommit() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, initialConfig, 42L);
        PublishRequest publishRequest = cs1.handleClientValue(state2);
        PublishResponse publishResponse = cs1.handlePublishRequest(publishRequest);
        Optional<ApplyCommitRequest> applyCommit = cs1.handlePublishResponse(node1, publishResponse);
        assertTrue(applyCommit.isPresent());
        assertThat(applyCommit.get().getSourceNode(), equalTo(node1));
        assertThat(applyCommit.get().getTerm(), equalTo(state2.term()));
        assertThat(applyCommit.get().getVersion(), equalTo(state2.version()));
    }

    public void testHandlePublishResponseWhenSteppedDownAsLeader() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, initialConfig, 42L);
        PublishRequest publishRequest = cs1.handleClientValue(state2);
        PublishResponse publishResponse = cs1.handlePublishRequest(publishRequest);
        StartJoinRequest startJoinRequest2 = new StartJoinRequest(node1, randomLongBetween(startJoinRequest1.getTerm() + 1, 10));
        cs1.handleStartJoin(startJoinRequest2);
        assertThat(
            expectThrows(
                CoordinationStateRejectedException.class,
                () -> cs1.handlePublishResponse(randomFrom(node1, node2, node3), publishResponse)
            ).getMessage(),
            containsString("election not won")
        );
    }

    public void testHandlePublishResponseWithoutPublishConfigQuorum() {
        VotingConfiguration configNode1 = VotingConfiguration.of(node1);
        VotingConfiguration configNode2 = VotingConfiguration.of(node2);
        ClusterState state1 = clusterState(0L, 0L, node1, configNode1, configNode1, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());
        Join v2 = cs2.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v2));
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, configNode1, configNode2, 42L);
        PublishRequest publishRequest = cs1.handleClientValue(state2);
        PublishResponse publishResponse = cs1.handlePublishRequest(publishRequest);
        Optional<ApplyCommitRequest> applyCommit = cs1.handlePublishResponse(node1, publishResponse);
        assertFalse(applyCommit.isPresent());
    }

    public void testHandlePublishResponseWithoutCommitedConfigQuorum() {
        VotingConfiguration configNode1 = VotingConfiguration.of(node1);
        VotingConfiguration configNode2 = VotingConfiguration.of(node2);
        ClusterState state1 = clusterState(0L, 0L, node1, configNode1, configNode1, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());
        Join v2 = cs2.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v2));
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, configNode1, configNode2, 42L);
        PublishRequest publishRequest = cs1.handleClientValue(state2);
        PublishResponse publishResponse = cs2.handlePublishRequest(publishRequest);
        Optional<ApplyCommitRequest> applyCommit = cs1.handlePublishResponse(node2, publishResponse);
        assertFalse(applyCommit.isPresent());
    }

    public void testHandlePublishResponseWithoutCommit() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, initialConfig, 42L);
        PublishRequest publishRequest = cs1.handleClientValue(state2);
        PublishResponse publishResponse = cs1.handlePublishRequest(publishRequest);
        Optional<ApplyCommitRequest> applyCommit = cs1.handlePublishResponse(node2, publishResponse);
        assertFalse(applyCommit.isPresent());
    }

    public void testHandlePublishResponseWithBadTerm() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(2, 10), node1, initialConfig, initialConfig, 42L);
        PublishResponse publishResponse = cs1.handlePublishRequest(new PublishRequest(state2));
        long term = randomBoolean()
            ? randomLongBetween(startJoinRequest1.getTerm() + 1, 10)
            : randomLongBetween(0, startJoinRequest1.getTerm() - 1);
        assertThat(
            expectThrows(
                CoordinationStateRejectedException.class,
                () -> cs1.handlePublishResponse(randomFrom(node1, node2, node3), new PublishResponse(term, publishResponse.getVersion()))
            ).getMessage(),
            containsString("does not match current term")
        );
    }

    public void testHandlePublishResponseWithVersionMismatch() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), randomLongBetween(2, 10), node1, initialConfig, initialConfig, 42L);
        PublishResponse publishResponse = cs1.handlePublishRequest(new PublishRequest(state2));
        assertThat(
            expectThrows(
                CoordinationStateRejectedException.class,
                () -> cs1.handlePublishResponse(randomFrom(node1, node2, node3), publishResponse)
            ).getMessage(),
            containsString("does not match current version")
        );
    }

    public void testHandleCommit() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());
        Join v2 = cs2.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v2));
        VotingConfiguration newConfig = VotingConfiguration.of(node2);
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, newConfig, 7L);
        PublishRequest publishRequest = cs1.handleClientValue(state2);
        PublishResponse publishResponse = cs1.handlePublishRequest(publishRequest);
        cs1.handlePublishResponse(node1, publishResponse);
        Optional<ApplyCommitRequest> applyCommit = cs1.handlePublishResponse(node2, publishResponse);
        assertTrue(applyCommit.isPresent());
        assertThat(cs1.getLastCommittedConfiguration(), equalTo(initialConfig));
        cs1.handleCommit(applyCommit.get());
        assertThat(cs1.getLastCommittedConfiguration(), equalTo(newConfig));
    }

    public void testHandleCommitWithBadCurrentTerm() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, initialConfig, 7L);
        PublishRequest publishRequest = cs1.handleClientValue(state2);
        PublishResponse publishResponse = cs1.handlePublishRequest(publishRequest);
        cs1.handlePublishResponse(node1, publishResponse);
        long term = randomBoolean()
            ? randomLongBetween(startJoinRequest1.getTerm() + 1, 10)
            : randomLongBetween(0, startJoinRequest1.getTerm() - 1);
        assertThat(
            expectThrows(CoordinationStateRejectedException.class, () -> cs1.handleCommit(new ApplyCommitRequest(node1, term, 2L)))
                .getMessage(),
            containsString("does not match current term")
        );
    }

    public void testHandleCommitWithBadLastAcceptedTerm() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());
        assertThat(
            expectThrows(
                CoordinationStateRejectedException.class,
                () -> cs1.handleCommit(new ApplyCommitRequest(node1, startJoinRequest1.getTerm(), 2L))
            ).getMessage(),
            containsString("does not match last accepted term")
        );
    }

    public void testHandleCommitWithBadVersion() {
        VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        ClusterState state1 = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        cs1.setInitialState(state1);
        StartJoinRequest startJoinRequest1 = new StartJoinRequest(node1, randomLongBetween(1, 5));
        Join v1 = cs1.handleStartJoin(startJoinRequest1);
        assertTrue(cs1.handleJoin(v1));
        assertTrue(cs1.electionWon());
        ClusterState state2 = clusterState(startJoinRequest1.getTerm(), 2L, node1, initialConfig, initialConfig, 7L);
        PublishRequest publishRequest = cs1.handleClientValue(state2);
        cs1.handlePublishRequest(publishRequest);
        assertThat(
            expectThrows(
                CoordinationStateRejectedException.class,
                () -> cs1.handleCommit(new ApplyCommitRequest(node1, startJoinRequest1.getTerm(), randomLongBetween(3, 10)))
            ).getMessage(),
            containsString("does not match current version")
        );
    }

    public void testVoteCollection() {
        final CoordinationState.VoteCollection voteCollection = new CoordinationState.VoteCollection();
        assertTrue(voteCollection.isEmpty());

        assertFalse(
            voteCollection.addVote(
                new DiscoveryNode("cluster-manager-ineligible", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT)
            )
        );
        assertTrue(voteCollection.isEmpty());

        voteCollection.addVote(node1);
        assertFalse(voteCollection.isEmpty());
        assertTrue(voteCollection.containsVoteFor(node1));
        assertFalse(voteCollection.containsVoteFor(node2));
        assertFalse(voteCollection.containsVoteFor(node3));
        voteCollection.addVote(node2);
        assertTrue(voteCollection.containsVoteFor(node1));
        assertTrue(voteCollection.containsVoteFor(node2));
        assertFalse(voteCollection.containsVoteFor(node3));
        assertTrue(voteCollection.isQuorum(VotingConfiguration.of(node1, node2)));
        assertTrue(voteCollection.isQuorum(VotingConfiguration.of(node1)));
        assertFalse(voteCollection.isQuorum(VotingConfiguration.of(node3)));

        EqualsHashCodeTestUtils.CopyFunction<CoordinationState.VoteCollection> copyFunction = vc -> {
            CoordinationState.VoteCollection voteCollection1 = new CoordinationState.VoteCollection();
            for (DiscoveryNode node : vc.nodes()) {
                voteCollection1.addVote(node);
            }
            return voteCollection1;
        };
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(voteCollection, copyFunction, vc -> {
            CoordinationState.VoteCollection copy = copyFunction.copy(vc);
            copy.addVote(createNode(randomAlphaOfLength(10)));
            return copy;
        });
    }

    public void testSafety() {
        new CoordinationStateTestCluster(
            IntStream.range(0, randomIntBetween(1, 5))
                .mapToObj(i -> new DiscoveryNode("node_" + i, buildNewFakeTransportAddress(), Version.CURRENT))
                .collect(Collectors.toList()),
            ElectionStrategy.DEFAULT_INSTANCE
        ).runRandomly();
    }

    public void testHandlePrePublishAndCommitWhenRemoteStateDisabled() {
        final PersistedStateRegistry persistedStateRegistry = persistedStateRegistry();
        persistedStateRegistry.addPersistedState(PersistedStateType.LOCAL, ps1);
        final PersistedStateRegistry persistedStateRegistrySpy = Mockito.spy(persistedStateRegistry);
        final CoordinationState coordinationState = createCoordinationState(persistedStateRegistrySpy, node1, Settings.EMPTY);
        final VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        final ClusterState clusterState = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        coordinationState.handlePrePublish(clusterState);
        Mockito.verify(persistedStateRegistrySpy, Mockito.times(0)).getPersistedState(PersistedStateType.REMOTE);
        assertThat(persistedStateRegistry.getPersistedState(PersistedStateType.REMOTE), nullValue());
        final ClusterState clusterState2 = clusterState(0L, 1L, node1, initialConfig, initialConfig, 42L);
        coordinationState.handlePrePublish(clusterState2);
        Mockito.verify(persistedStateRegistrySpy, Mockito.times(0)).getPersistedState(PersistedStateType.REMOTE);
        assertThat(persistedStateRegistry.getPersistedState(PersistedStateType.REMOTE), nullValue());
        coordinationState.handlePreCommit();
        Mockito.verify(persistedStateRegistrySpy, Mockito.times(0)).getPersistedState(PersistedStateType.REMOTE);
        assertThat(persistedStateRegistry.getPersistedState(PersistedStateType.REMOTE), nullValue());
    }

    public void testHandlePrePublishAndCommitWhenRemoteStateEnabled() throws IOException {
        final RemoteClusterStateService remoteClusterStateService = Mockito.mock(RemoteClusterStateService.class);
        final VotingConfiguration initialConfig = VotingConfiguration.of(node1);
        final ClusterState clusterState = clusterState(0L, 0L, node1, initialConfig, initialConfig, 42L);
        final String previousClusterUUID = "prev-cluster-uuid";
        final ClusterMetadataManifest manifest = new ClusterMetadataManifest(
            0L,
            0L,
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            Version.CURRENT,
            randomAlphaOfLength(10),
            false,
            1,
            randomAlphaOfLength(10),
            Collections.emptyList(),
            randomAlphaOfLength(10),
            true
        );
        Mockito.when(remoteClusterStateService.writeFullMetadata(clusterState, previousClusterUUID)).thenReturn(manifest);

        final PersistedStateRegistry persistedStateRegistry = persistedStateRegistry();
        persistedStateRegistry.addPersistedState(PersistedStateType.LOCAL, ps1);
        persistedStateRegistry.addPersistedState(
            PersistedStateType.REMOTE,
            new RemotePersistedState(remoteClusterStateService, previousClusterUUID)
        );

        String randomRepoName = "randomRepoName";
        String stateRepoTypeAttributeKey = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            randomRepoName
        );
        String stateRepoSettingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            randomRepoName
        );

        Settings settings = Settings.builder()
            .put("node.attr." + REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY, randomRepoName)
            .put(stateRepoTypeAttributeKey, FsRepository.TYPE)
            .put(stateRepoSettingsAttributeKeyPrefix + "location", "randomRepoPath")
            .put(RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .build();

        final CoordinationState coordinationState = createCoordinationState(persistedStateRegistry, node1, settings);
        coordinationState.handlePrePublish(clusterState);
        Mockito.verify(remoteClusterStateService, Mockito.times(1)).writeFullMetadata(clusterState, previousClusterUUID);
        assertThat(persistedStateRegistry.getPersistedState(PersistedStateType.REMOTE).getLastAcceptedState(), equalTo(clusterState));

        coordinationState.handlePreCommit();
        ClusterState committedClusterState = ClusterState.builder(clusterState)
            .metadata(Metadata.builder(clusterState.metadata()).clusterUUIDCommitted(true).build())
            .build();
        // Mockito.verify(remoteClusterStateService, Mockito.times(1)).markLastStateAsCommitted(committedClusterState, manifest);
        ArgumentCaptor<ClusterState> clusterStateCaptor = ArgumentCaptor.forClass(ClusterState.class);
        verify(remoteClusterStateService, times(1)).markLastStateAsCommitted(clusterStateCaptor.capture(), any());
        assertThat(clusterStateCaptor.getValue().metadata().indices(), equalTo(committedClusterState.metadata().indices()));
        assertThat(clusterStateCaptor.getValue().metadata().clusterUUID(), equalTo(committedClusterState.metadata().clusterUUID()));
        assertThat(clusterStateCaptor.getValue().stateUUID(), equalTo(committedClusterState.stateUUID()));
        assertThat(
            clusterStateCaptor.getValue().coordinationMetadata().term(),
            equalTo(committedClusterState.coordinationMetadata().term())
        );
        assertThat(clusterStateCaptor.getValue().version(), equalTo(committedClusterState.version()));
        assertThat(
            clusterStateCaptor.getValue().metadata().clusterUUIDCommitted(),
            equalTo(committedClusterState.metadata().clusterUUIDCommitted())
        );
    }

    public static CoordinationState createCoordinationState(
        PersistedStateRegistry persistedStateRegistry,
        DiscoveryNode localNode,
        Settings settings
    ) {
        return new CoordinationState(localNode, persistedStateRegistry, ElectionStrategy.DEFAULT_INSTANCE, settings);
    }

    public static ClusterState clusterState(
        long term,
        long version,
        DiscoveryNode localNode,
        VotingConfiguration lastCommittedConfig,
        VotingConfiguration lastAcceptedConfig,
        long value
    ) {
        return clusterState(
            term,
            version,
            DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).build(),
            lastCommittedConfig,
            lastAcceptedConfig,
            value
        );
    }

    public static ClusterState clusterState(
        long term,
        long version,
        DiscoveryNodes discoveryNodes,
        VotingConfiguration lastCommittedConfig,
        VotingConfiguration lastAcceptedConfig,
        long value
    ) {
        return setValue(
            ClusterState.builder(ClusterName.DEFAULT)
                .version(version)
                .nodes(discoveryNodes)
                .metadata(
                    Metadata.builder()
                        .clusterUUID(UUIDs.randomBase64UUID(random())) // generate cluster UUID deterministically for repeatable tests
                        .coordinationMetadata(
                            CoordinationMetadata.builder()
                                .term(term)
                                .lastCommittedConfiguration(lastCommittedConfig)
                                .lastAcceptedConfiguration(lastAcceptedConfig)
                                .build()
                        )
                )
                .stateUUID(UUIDs.randomBase64UUID(random())) // generate cluster state UUID deterministically for repeatable tests
                .build(),
            value
        );
    }

    public static ClusterState setValue(ClusterState clusterState, long value) {
        return ClusterState.builder(clusterState)
            .metadata(
                Metadata.builder(clusterState.metadata())
                    .persistentSettings(Settings.builder().put(clusterState.metadata().persistentSettings()).put("value", value).build())
                    .build()
            )
            .build();
    }

    public static long value(ClusterState clusterState) {
        return clusterState.metadata().persistentSettings().getAsLong("value", 0L);
    }

    private static PersistedStateRegistry createPersistedStateRegistry(ClusterState clusterState) {
        final PersistedStateRegistry persistedStateRegistry = new PersistedStateRegistry();
        persistedStateRegistry.addPersistedState(PersistedStateType.LOCAL, new InMemoryPersistedState(0L, clusterState));
        return persistedStateRegistry;
    }
}
