/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.replication;

import org.opensearch.core.action.ActionListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.CheckpointInfoResponse;
import org.opensearch.indices.replication.GetSegmentFilesResponse;
import org.opensearch.indices.replication.SegmentReplicationSource;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

import java.util.List;
import java.util.function.BiConsumer;

/**
 * This class is used by unit tests implementing SegmentReplicationSource
 */
public abstract class TestReplicationSource implements SegmentReplicationSource {

    @Override
    public abstract void getCheckpointMetadata(
        long replicationId,
        ReplicationCheckpoint checkpoint,
        ActionListener<CheckpointInfoResponse> listener
    );

    @Override
    public abstract void getSegmentFiles(
        long replicationId,
        ReplicationCheckpoint checkpoint,
        List<StoreFileMetadata> filesToFetch,
        IndexShard indexShard,
        BiConsumer<String, Long> fileProgressTracker,
        ActionListener<GetSegmentFilesResponse> listener
    );

    @Override
    public String getDescription() {
        return "This is a test description";
    }
}
