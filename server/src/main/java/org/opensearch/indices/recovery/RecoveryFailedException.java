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

package org.opensearch.indices.recovery;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.replication.common.ReplicationFailedException;

import java.io.IOException;

/**
 * Exception thrown if recovery fails
 *
 * @opensearch.internal
 */
public class RecoveryFailedException extends ReplicationFailedException {

    public RecoveryFailedException(StartRecoveryRequest request, Throwable cause) {
        this(request, null, cause);
    }

    public RecoveryFailedException(StartRecoveryRequest request, @Nullable String extraInfo, Throwable cause) {
        this(request.shardId(), request.sourceNode(), request.targetNode(), extraInfo, cause);
    }

    public RecoveryFailedException(RecoveryState state, @Nullable String extraInfo, Throwable cause) {
        this(state.getShardId(), state.getSourceNode(), state.getTargetNode(), extraInfo, cause);
    }

    public RecoveryFailedException(
        ShardId shardId,
        DiscoveryNode sourceNode,
        DiscoveryNode targetNode,
        @Nullable String extraInfo,
        Throwable cause
    ) {
        super(
            shardId
                + ": Recovery failed "
                + (sourceNode != null ? "from " + sourceNode + " into " : "on ")
                + targetNode
                + (extraInfo == null ? "" : " (" + extraInfo + ")"),
            cause
        );
    }

    public RecoveryFailedException(StreamInput in) throws IOException {
        super(in);
    }
}
