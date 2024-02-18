/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.index.shard;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;

/**
 * Exception to indicate failures are caused due to the closure of the primary
 * shard.
 *
 * @opensearch.internal
 */
public class PrimaryShardClosedException extends IndexShardClosedException {
    public PrimaryShardClosedException(ShardId shardId) {
        super(shardId, "Primary closed");
    }

    public PrimaryShardClosedException(StreamInput in) throws IOException {
        super(in);
    }
}
