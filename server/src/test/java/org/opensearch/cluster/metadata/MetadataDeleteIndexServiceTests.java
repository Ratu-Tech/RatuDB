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

package org.opensearch.cluster.metadata;

import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.DataStreamTestHelper;
import org.opensearch.cluster.SnapshotsInProgress;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.repositories.IndexId;
import org.opensearch.snapshots.Snapshot;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.snapshots.SnapshotInProgressException;
import org.opensearch.snapshots.SnapshotInfoTests;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;
import org.hamcrest.core.IsNull;
import org.junit.Before;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MetadataDeleteIndexServiceTests extends OpenSearchTestCase {
    private AllocationService allocationService;
    private MetadataDeleteIndexService service;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        allocationService = mock(AllocationService.class);
        when(allocationService.reroute(any(ClusterState.class), any(String.class))).thenAnswer(
            mockInvocation -> mockInvocation.getArguments()[0]
        );
        service = new MetadataDeleteIndexService(Settings.EMPTY, mock(ClusterService.class), allocationService);
    }

    public void testDeleteMissing() {
        Index index = new Index("missing", "doesn't matter");
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class, () -> service.deleteIndices(state, singleton(index)));
        assertEquals(index, e.getIndex());
    }

    public void testDeleteSnapshotting() {
        String index = randomAlphaOfLength(5);
        Snapshot snapshot = new Snapshot("doesn't matter", new SnapshotId("snapshot name", "snapshot uuid"));
        SnapshotsInProgress snaps = SnapshotsInProgress.of(
            List.of(
                new SnapshotsInProgress.Entry(
                    snapshot,
                    true,
                    false,
                    SnapshotsInProgress.State.INIT,
                    singletonList(new IndexId(index, "doesn't matter")),
                    Collections.emptyList(),
                    System.currentTimeMillis(),
                    (long) randomIntBetween(0, 1000),
                    Map.of(),
                    null,
                    SnapshotInfoTests.randomUserMetadata(),
                    VersionUtils.randomVersion(random()),
                    false
                )
            )
        );
        ClusterState state = ClusterState.builder(clusterState(index)).putCustom(SnapshotsInProgress.TYPE, snaps).build();
        Exception e = expectThrows(
            SnapshotInProgressException.class,
            () -> service.deleteIndices(state, singleton(state.metadata().getIndices().get(index).getIndex()))
        );
        assertEquals(
            "Cannot delete indices that are being snapshotted: [["
                + index
                + "]]. Try again after snapshot finishes "
                + "or cancel the currently running snapshot.",
            e.getMessage()
        );
    }

    public void testDeleteUnassigned() {
        // Create an unassigned index
        String index = randomAlphaOfLength(5);
        ClusterState before = clusterState(index);

        // Mock the built reroute
        when(allocationService.reroute(any(ClusterState.class), any(String.class))).then(i -> i.getArguments()[0]);

        // Remove it
        ClusterState after = service.deleteIndices(before, singleton(before.metadata().getIndices().get(index).getIndex()));

        // It is gone
        assertNull(after.metadata().getIndices().get(index));
        assertNull(after.routingTable().index(index));
        assertNull(after.blocks().indices().get(index));

        // Make sure we actually attempted to reroute
        verify(allocationService).reroute(any(ClusterState.class), any(String.class));
    }

    public void testDeleteBackingIndexForDataStream() {
        int numBackingIndices = randomIntBetween(2, 5);
        String dataStreamName = randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        ClusterState before = DataStreamTestHelper.getClusterStateWithDataStreams(
            List.of(new Tuple<>(dataStreamName, numBackingIndices)),
            List.of()
        );

        int numIndexToDelete = randomIntBetween(1, numBackingIndices - 1);

        Index indexToDelete = before.metadata().index(DataStream.getDefaultBackingIndexName(dataStreamName, numIndexToDelete)).getIndex();
        ClusterState after = service.deleteIndices(before, Set.of(indexToDelete));

        assertThat(after.metadata().getIndices().get(indexToDelete.getName()), IsNull.nullValue());
        assertThat(after.metadata().getIndices().size(), equalTo(numBackingIndices - 1));
        assertThat(
            after.metadata().getIndices().get(DataStream.getDefaultBackingIndexName(dataStreamName, numIndexToDelete)),
            IsNull.nullValue()
        );
    }

    public void testDeleteMultipleBackingIndexForDataStream() {
        int numBackingIndices = randomIntBetween(3, 5);
        int numBackingIndicesToDelete = randomIntBetween(2, numBackingIndices - 1);
        String dataStreamName = randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        ClusterState before = DataStreamTestHelper.getClusterStateWithDataStreams(
            List.of(new Tuple<>(dataStreamName, numBackingIndices)),
            List.of()
        );

        List<Integer> indexNumbersToDelete = randomSubsetOf(
            numBackingIndicesToDelete,
            IntStream.rangeClosed(1, numBackingIndices - 1).boxed().collect(Collectors.toList())
        );

        Set<Index> indicesToDelete = new HashSet<>();
        for (int k : indexNumbersToDelete) {
            indicesToDelete.add(before.metadata().index(DataStream.getDefaultBackingIndexName(dataStreamName, k)).getIndex());
        }
        ClusterState after = service.deleteIndices(before, indicesToDelete);

        DataStream dataStream = after.metadata().dataStreams().get(dataStreamName);
        assertThat(dataStream, IsNull.notNullValue());
        assertThat(dataStream.getIndices().size(), equalTo(numBackingIndices - indexNumbersToDelete.size()));
        for (Index i : indicesToDelete) {
            assertThat(after.metadata().getIndices().get(i.getName()), IsNull.nullValue());
            assertFalse(dataStream.getIndices().contains(i));
        }
        assertThat(after.metadata().getIndices().size(), equalTo(numBackingIndices - indexNumbersToDelete.size()));
    }

    public void testDeleteCurrentWriteIndexForDataStream() {
        int numBackingIndices = randomIntBetween(1, 5);
        String dataStreamName = randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        ClusterState before = DataStreamTestHelper.getClusterStateWithDataStreams(
            List.of(new Tuple<>(dataStreamName, numBackingIndices)),
            List.of()
        );

        Index indexToDelete = before.metadata().index(DataStream.getDefaultBackingIndexName(dataStreamName, numBackingIndices)).getIndex();
        Exception e = expectThrows(IllegalArgumentException.class, () -> service.deleteIndices(before, Set.of(indexToDelete)));

        assertThat(
            e.getMessage(),
            containsString(
                "index [" + indexToDelete.getName() + "] is the write index for data stream [" + dataStreamName + "] and cannot be deleted"
            )
        );
    }

    private ClusterState clusterState(String index) {
        IndexMetadata indexMetadata = IndexMetadata.builder(index)
            .settings(Settings.builder().put("index.version.created", VersionUtils.randomVersion(random())))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder().addAsNew(indexMetadata).build())
            .blocks(ClusterBlocks.builder().addBlocks(indexMetadata))
            .build();
    }
}
