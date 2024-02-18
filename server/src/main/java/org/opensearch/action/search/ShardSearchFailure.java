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

package org.opensearch.action.search;

import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.action.OriginalIndices;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Nullable;
import org.opensearch.core.action.ShardOperationFailedException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchException;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.transport.RemoteClusterAware;

import java.io.IOException;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Represents a failure to search on a specific shard.
 *
 * @opensearch.internal
 */
public class ShardSearchFailure extends ShardOperationFailedException {

    private static final String REASON_FIELD = "reason";
    private static final String NODE_FIELD = "node";
    private static final String INDEX_FIELD = "index";
    private static final String SHARD_FIELD = "shard";

    public static final ShardSearchFailure[] EMPTY_ARRAY = new ShardSearchFailure[0];

    private SearchShardTarget shardTarget;

    ShardSearchFailure(StreamInput in) throws IOException {
        shardTarget = in.readOptionalWriteable(SearchShardTarget::new);
        if (shardTarget != null) {
            index = shardTarget.getFullyQualifiedIndexName();
            shardId = shardTarget.getShardId().getId();
        }
        reason = in.readString();
        status = RestStatus.readFrom(in);
        cause = in.readException();
    }

    public ShardSearchFailure(Exception e) {
        this(e, null);
    }

    public ShardSearchFailure(Exception e, @Nullable SearchShardTarget shardTarget) {
        this(e, ExceptionsHelper.unwrapCause(e), shardTarget);
    }

    private ShardSearchFailure(final Exception e, final Throwable unwrappedCause, @Nullable SearchShardTarget shardTarget) {
        super(
            shardTarget == null ? null : shardTarget.getFullyQualifiedIndexName(),
            shardTarget == null ? -1 : shardTarget.getShardId().getId(),
            ExceptionsHelper.detailedMessage(e),
            ExceptionsHelper.status(unwrappedCause),
            unwrappedCause
        );

        if (unwrappedCause instanceof SearchException) {
            this.shardTarget = ((SearchException) unwrappedCause).shard();
        } else if (shardTarget != null) {
            this.shardTarget = shardTarget;
        }
    }

    /**
     * The search shard target the failure occurred on.
     */
    @Nullable
    public SearchShardTarget shard() {
        return this.shardTarget;
    }

    @Override
    public String toString() {
        return "shard ["
            + (shardTarget == null ? "_na" : shardTarget)
            + "], reason ["
            + reason
            + "], cause ["
            + (cause == null ? "_na" : ExceptionsHelper.stackTrace(cause))
            + "]";
    }

    public static ShardSearchFailure readShardSearchFailure(StreamInput in) throws IOException {
        return new ShardSearchFailure(in);

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(shardTarget);
        out.writeString(reason);
        RestStatus.writeTo(out, status);
        out.writeException(cause);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(SHARD_FIELD, shardId());
            builder.field(INDEX_FIELD, index());
            if (shardTarget != null) {
                builder.field(NODE_FIELD, shardTarget.getNodeId());
            }
            builder.field(REASON_FIELD);
            builder.startObject();
            OpenSearchException.generateThrowableXContent(builder, params, cause);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    public static ShardSearchFailure fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token;
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        String currentFieldName = null;
        int shardId = -1;
        String indexName = null;
        String clusterAlias = null;
        String nodeId = null;
        OpenSearchException exception = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (SHARD_FIELD.equals(currentFieldName)) {
                    shardId = parser.intValue();
                } else if (INDEX_FIELD.equals(currentFieldName)) {
                    indexName = parser.text();
                    int indexOf = indexName.indexOf(RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR);
                    if (indexOf > 0) {
                        clusterAlias = indexName.substring(0, indexOf);
                        indexName = indexName.substring(indexOf + 1);
                    }
                } else if (NODE_FIELD.equals(currentFieldName)) {
                    nodeId = parser.text();
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (REASON_FIELD.equals(currentFieldName)) {
                    exception = OpenSearchException.fromXContent(parser);
                } else {
                    parser.skipChildren();
                }
            } else {
                parser.skipChildren();
            }
        }
        SearchShardTarget searchShardTarget = null;
        if (nodeId != null) {
            searchShardTarget = new SearchShardTarget(
                nodeId,
                new ShardId(new Index(indexName, IndexMetadata.INDEX_UUID_NA_VALUE), shardId),
                clusterAlias,
                OriginalIndices.NONE
            );
        }
        return new ShardSearchFailure(exception, searchShardTarget);
    }
}
