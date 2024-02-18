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

package org.opensearch.action.admin.cluster.snapshots.status;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.StepListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.SnapshotsInProgress;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.snapshots.IndexShardSnapshotStatus;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.snapshots.Snapshot;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.snapshots.SnapshotInfo;
import org.opensearch.snapshots.SnapshotMissingException;
import org.opensearch.snapshots.SnapshotShardFailure;
import org.opensearch.snapshots.SnapshotShardsService;
import org.opensearch.snapshots.SnapshotState;
import org.opensearch.snapshots.SnapshotsService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableMap;

/**
 * Transport action for accessing snapshot status
 *
 * @opensearch.internal
 */
public class TransportSnapshotsStatusAction extends TransportClusterManagerNodeAction<SnapshotsStatusRequest, SnapshotsStatusResponse> {

    private static final Logger logger = LogManager.getLogger(TransportSnapshotsStatusAction.class);

    private final RepositoriesService repositoriesService;

    private final TransportNodesSnapshotsStatus transportNodesSnapshotsStatus;

    @Inject
    public TransportSnapshotsStatusAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        RepositoriesService repositoriesService,
        TransportNodesSnapshotsStatus transportNodesSnapshotsStatus,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            SnapshotsStatusAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            SnapshotsStatusRequest::new,
            indexNameExpressionResolver
        );
        this.repositoriesService = repositoriesService;
        this.transportNodesSnapshotsStatus = transportNodesSnapshotsStatus;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected ClusterBlockException checkBlock(SnapshotsStatusRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected SnapshotsStatusResponse read(StreamInput in) throws IOException {
        return new SnapshotsStatusResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        final SnapshotsStatusRequest request,
        final ClusterState state,
        final ActionListener<SnapshotsStatusResponse> listener
    ) throws Exception {
        final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
        List<SnapshotsInProgress.Entry> currentSnapshots = SnapshotsService.currentSnapshots(
            snapshotsInProgress,
            request.repository(),
            Arrays.asList(request.snapshots())
        );
        if (currentSnapshots.isEmpty()) {
            buildResponse(snapshotsInProgress, request, currentSnapshots, null, listener);
            return;
        }

        Set<String> nodesIds = new HashSet<>();
        for (SnapshotsInProgress.Entry entry : currentSnapshots) {
            for (final SnapshotsInProgress.ShardSnapshotStatus status : entry.shards().values()) {
                if (status.nodeId() != null) {
                    nodesIds.add(status.nodeId());
                }
            }
        }

        if (!nodesIds.isEmpty()) {
            // There are still some snapshots running - check their progress
            Snapshot[] snapshots = new Snapshot[currentSnapshots.size()];
            for (int i = 0; i < currentSnapshots.size(); i++) {
                snapshots[i] = currentSnapshots.get(i).snapshot();
            }
            transportNodesSnapshotsStatus.execute(
                new TransportNodesSnapshotsStatus.Request(nodesIds.toArray(Strings.EMPTY_ARRAY)).snapshots(snapshots)
                    .timeout(request.clusterManagerNodeTimeout()),
                ActionListener.wrap(
                    nodeSnapshotStatuses -> threadPool.generic()
                        .execute(
                            ActionRunnable.wrap(
                                listener,
                                l -> buildResponse(snapshotsInProgress, request, currentSnapshots, nodeSnapshotStatuses, l)
                            )
                        ),
                    listener::onFailure
                )
            );
        } else {
            // We don't have any in-progress shards, just return current stats
            buildResponse(snapshotsInProgress, request, currentSnapshots, null, listener);
        }

    }

    private void buildResponse(
        SnapshotsInProgress snapshotsInProgress,
        SnapshotsStatusRequest request,
        List<SnapshotsInProgress.Entry> currentSnapshotEntries,
        TransportNodesSnapshotsStatus.NodesSnapshotStatus nodeSnapshotStatuses,
        ActionListener<SnapshotsStatusResponse> listener
    ) {
        // First process snapshot that are currently processed
        List<SnapshotStatus> builder = new ArrayList<>();
        Set<String> currentSnapshotNames = new HashSet<>();
        if (!currentSnapshotEntries.isEmpty()) {
            Map<String, TransportNodesSnapshotsStatus.NodeSnapshotStatus> nodeSnapshotStatusMap;
            if (nodeSnapshotStatuses != null) {
                nodeSnapshotStatusMap = nodeSnapshotStatuses.getNodesMap();
            } else {
                nodeSnapshotStatusMap = new HashMap<>();
            }

            for (SnapshotsInProgress.Entry entry : currentSnapshotEntries) {
                currentSnapshotNames.add(entry.snapshot().getSnapshotId().getName());
                List<SnapshotIndexShardStatus> shardStatusBuilder = new ArrayList<>();
                Map<String, IndexId> indexIdLookup = null;
                for (final Map.Entry<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shardEntry : entry.shards().entrySet()) {
                    SnapshotsInProgress.ShardSnapshotStatus status = shardEntry.getValue();
                    if (status.nodeId() != null) {
                        // We should have information about this shard from the shard:
                        TransportNodesSnapshotsStatus.NodeSnapshotStatus nodeStatus = nodeSnapshotStatusMap.get(status.nodeId());
                        if (nodeStatus != null) {
                            Map<ShardId, SnapshotIndexShardStatus> shardStatues = nodeStatus.status().get(entry.snapshot());
                            if (shardStatues != null) {
                                SnapshotIndexShardStatus shardStatus = shardStatues.get(shardEntry.getKey());
                                if (shardStatus != null) {
                                    // We have full information about this shard
                                    if (shardStatus.getStage() == SnapshotIndexShardStage.DONE
                                        && shardEntry.getValue().state() != SnapshotsInProgress.ShardState.SUCCESS) {
                                        // Unlikely edge case:
                                        // Data node has finished snapshotting the shard but the cluster state has not yet been updated
                                        // to reflect this. We adjust the status to show up as snapshot metadata being written because
                                        // technically if the data node failed before successfully reporting DONE state to cluster-manager,
                                        // then this shards state would jump to a failed state.
                                        shardStatus = new SnapshotIndexShardStatus(
                                            shardEntry.getKey(),
                                            SnapshotIndexShardStage.FINALIZE,
                                            shardStatus.getStats(),
                                            shardStatus.getNodeId(),
                                            shardStatus.getFailure()
                                        );
                                    }
                                    shardStatusBuilder.add(shardStatus);
                                    continue;
                                }
                            }
                        }
                    }
                    // We failed to find the status of the shard from the responses we received from data nodes.
                    // This can happen if nodes drop out of the cluster completely or restart during the snapshot.
                    // We rebuild the information they would have provided from their in memory state from the cluster
                    // state and the repository contents in the below logic
                    final SnapshotIndexShardStage stage;
                    switch (shardEntry.getValue().state()) {
                        case FAILED:
                        case ABORTED:
                        case MISSING:
                            stage = SnapshotIndexShardStage.FAILURE;
                            break;
                        case INIT:
                        case WAITING:
                        case QUEUED:
                            stage = SnapshotIndexShardStage.STARTED;
                            break;
                        case SUCCESS:
                            stage = SnapshotIndexShardStage.DONE;
                            break;
                        default:
                            throw new IllegalArgumentException("Unknown snapshot state " + shardEntry.getValue().state());
                    }
                    final SnapshotIndexShardStatus shardStatus;
                    if (stage == SnapshotIndexShardStage.DONE) {
                        // Shard snapshot completed successfully so we should be able to load the exact statistics for this
                        // shard from the repository already.
                        if (indexIdLookup == null) {
                            indexIdLookup = entry.indices().stream().collect(Collectors.toMap(IndexId::getName, Function.identity()));
                        }
                        final ShardId shardId = shardEntry.getKey();
                        shardStatus = new SnapshotIndexShardStatus(
                            shardId,
                            repositoriesService.repository(entry.repository())
                                .getShardSnapshotStatus(
                                    entry.snapshot().getSnapshotId(),
                                    indexIdLookup.get(shardId.getIndexName()),
                                    shardId
                                )
                                .asCopy()
                        );
                    } else {
                        shardStatus = new SnapshotIndexShardStatus(shardEntry.getKey(), stage);
                    }
                    shardStatusBuilder.add(shardStatus);
                }
                builder.add(
                    new SnapshotStatus(
                        entry.snapshot(),
                        entry.state(),
                        Collections.unmodifiableList(shardStatusBuilder),
                        entry.includeGlobalState(),
                        entry.startTime(),
                        Math.max(threadPool.absoluteTimeInMillis() - entry.startTime(), 0L)
                    )
                );
            }
        }
        // Now add snapshots on disk that are not currently running
        final String repositoryName = request.repository();
        if (Strings.hasText(repositoryName) && CollectionUtils.isEmpty(request.snapshots()) == false) {
            loadRepositoryData(snapshotsInProgress, request, builder, currentSnapshotNames, repositoryName, listener);
        } else {
            listener.onResponse(new SnapshotsStatusResponse(Collections.unmodifiableList(builder)));
        }
    }

    private void loadRepositoryData(
        SnapshotsInProgress snapshotsInProgress,
        SnapshotsStatusRequest request,
        List<SnapshotStatus> builder,
        Set<String> currentSnapshotNames,
        String repositoryName,
        ActionListener<SnapshotsStatusResponse> listener
    ) {
        final Set<String> requestedSnapshotNames = Sets.newHashSet(request.snapshots());
        final StepListener<RepositoryData> repositoryDataListener = new StepListener<>();
        repositoriesService.getRepositoryData(repositoryName, repositoryDataListener);
        repositoryDataListener.whenComplete(repositoryData -> {
            final Map<String, SnapshotId> matchedSnapshotIds = repositoryData.getSnapshotIds()
                .stream()
                .filter(s -> requestedSnapshotNames.contains(s.getName()))
                .collect(Collectors.toMap(SnapshotId::getName, Function.identity()));
            for (final String snapshotName : request.snapshots()) {
                if (currentSnapshotNames.contains(snapshotName)) {
                    // we've already found this snapshot in the current snapshot entries, so skip over
                    continue;
                }
                SnapshotId snapshotId = matchedSnapshotIds.get(snapshotName);
                if (snapshotId == null) {
                    // neither in the current snapshot entries nor found in the repository
                    if (request.ignoreUnavailable()) {
                        // ignoring unavailable snapshots, so skip over
                        logger.debug(
                            "snapshot status request ignoring snapshot [{}], not found in repository [{}]",
                            snapshotName,
                            repositoryName
                        );
                        continue;
                    } else {
                        throw new SnapshotMissingException(repositoryName, snapshotName);
                    }
                }
                SnapshotInfo snapshotInfo = snapshot(snapshotsInProgress, repositoryName, snapshotId);
                List<SnapshotIndexShardStatus> shardStatusBuilder = new ArrayList<>();
                if (snapshotInfo.state().completed()) {
                    Map<ShardId, IndexShardSnapshotStatus> shardStatuses = snapshotShards(repositoryName, repositoryData, snapshotInfo);
                    for (Map.Entry<ShardId, IndexShardSnapshotStatus> shardStatus : shardStatuses.entrySet()) {
                        IndexShardSnapshotStatus.Copy lastSnapshotStatus = shardStatus.getValue().asCopy();
                        shardStatusBuilder.add(new SnapshotIndexShardStatus(shardStatus.getKey(), lastSnapshotStatus));
                    }
                    final SnapshotsInProgress.State state;
                    switch (snapshotInfo.state()) {
                        case FAILED:
                            state = SnapshotsInProgress.State.FAILED;
                            break;
                        case SUCCESS:
                        case PARTIAL:
                            // Translating both PARTIAL and SUCCESS to SUCCESS for now
                            // TODO: add the differentiation on the metadata level in the next major release
                            state = SnapshotsInProgress.State.SUCCESS;
                            break;
                        default:
                            throw new IllegalArgumentException("Unknown snapshot state " + snapshotInfo.state());
                    }
                    final long startTime = snapshotInfo.startTime();
                    final long endTime = snapshotInfo.endTime();
                    assert endTime >= startTime || (endTime == 0L && snapshotInfo.state().completed() == false)
                        : "Inconsistent timestamps found in SnapshotInfo [" + snapshotInfo + "]";
                    builder.add(
                        new SnapshotStatus(
                            new Snapshot(repositoryName, snapshotId),
                            state,
                            Collections.unmodifiableList(shardStatusBuilder),
                            snapshotInfo.includeGlobalState(),
                            startTime,
                            // Use current time to calculate overall runtime for in-progress snapshots that have endTime == 0
                            (endTime == 0 ? threadPool.absoluteTimeInMillis() : endTime) - startTime
                        )
                    );
                }
            }
            listener.onResponse(new SnapshotsStatusResponse(Collections.unmodifiableList(builder)));
        }, listener::onFailure);
    }

    /**
     * Retrieves snapshot from repository
     *
     * @param snapshotsInProgress snapshots in progress in the cluster state
     * @param repositoryName      repository name
     * @param snapshotId          snapshot id
     * @return snapshot
     * @throws SnapshotMissingException if snapshot is not found
     */
    private SnapshotInfo snapshot(SnapshotsInProgress snapshotsInProgress, String repositoryName, SnapshotId snapshotId) {
        List<SnapshotsInProgress.Entry> entries = SnapshotsService.currentSnapshots(
            snapshotsInProgress,
            repositoryName,
            Collections.singletonList(snapshotId.getName())
        );
        if (!entries.isEmpty()) {
            return new SnapshotInfo(entries.iterator().next());
        }
        return repositoriesService.repository(repositoryName).getSnapshotInfo(snapshotId);
    }

    /**
     * Returns status of shards currently finished snapshots
     * <p>
     * This method is executed on cluster-manager node and it's complimentary to the
     * {@link SnapshotShardsService#currentSnapshotShards(Snapshot)} because it
     * returns similar information but for already finished snapshots.
     * </p>
     *
     * @param repositoryName  repository name
     * @param snapshotInfo    snapshot info
     * @return map of shard id to snapshot status
     */
    private Map<ShardId, IndexShardSnapshotStatus> snapshotShards(
        final String repositoryName,
        final RepositoryData repositoryData,
        final SnapshotInfo snapshotInfo
    ) throws IOException {
        final Repository repository = repositoriesService.repository(repositoryName);
        final Map<ShardId, IndexShardSnapshotStatus> shardStatus = new HashMap<>();
        for (String index : snapshotInfo.indices()) {
            IndexId indexId = repositoryData.resolveIndexId(index);
            IndexMetadata indexMetadata = repository.getSnapshotIndexMetaData(repositoryData, snapshotInfo.snapshotId(), indexId);
            if (indexMetadata != null) {
                int numberOfShards = indexMetadata.getNumberOfShards();
                for (int i = 0; i < numberOfShards; i++) {
                    ShardId shardId = new ShardId(indexMetadata.getIndex(), i);
                    SnapshotShardFailure shardFailure = findShardFailure(snapshotInfo.shardFailures(), shardId);
                    if (shardFailure != null) {
                        shardStatus.put(shardId, IndexShardSnapshotStatus.newFailed(shardFailure.reason()));
                    } else {
                        final IndexShardSnapshotStatus shardSnapshotStatus;
                        if (snapshotInfo.state() == SnapshotState.FAILED) {
                            // If the snapshot failed, but the shard's snapshot does
                            // not have an exception, it means that partial snapshots
                            // were disabled and in this case, the shard snapshot will
                            // *not* have any metadata, so attempting to read the shard
                            // snapshot status will throw an exception. Instead, we create
                            // a status for the shard to indicate that the shard snapshot
                            // could not be taken due to partial being set to false.
                            shardSnapshotStatus = IndexShardSnapshotStatus.newFailed("skipped");
                        } else {
                            shardSnapshotStatus = repository.getShardSnapshotStatus(snapshotInfo.snapshotId(), indexId, shardId);
                        }
                        shardStatus.put(shardId, shardSnapshotStatus);
                    }
                }
            }
        }
        return unmodifiableMap(shardStatus);
    }

    private static SnapshotShardFailure findShardFailure(List<SnapshotShardFailure> shardFailures, ShardId shardId) {
        for (SnapshotShardFailure shardFailure : shardFailures) {
            if (shardId.getIndexName().equals(shardFailure.index()) && shardId.getId() == shardFailure.shardId()) {
                return shardFailure;
            }
        }
        return null;
    }
}
