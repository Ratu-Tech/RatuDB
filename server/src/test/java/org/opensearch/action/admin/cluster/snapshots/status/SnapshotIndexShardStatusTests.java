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

package org.opensearch.action.admin.cluster.snapshots.status;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParserUtils;
import org.opensearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.function.Predicate;

public class SnapshotIndexShardStatusTests extends AbstractXContentTestCase<SnapshotIndexShardStatus> {

    @Override
    protected SnapshotIndexShardStatus createTestInstance() {
        return createForIndex(randomAlphaOfLength(10));
    }

    protected SnapshotIndexShardStatus createForIndex(String indexName) {
        ShardId shardId = new ShardId(new Index(indexName, IndexMetadata.INDEX_UUID_NA_VALUE), randomIntBetween(0, 500));
        SnapshotIndexShardStage stage = randomFrom(SnapshotIndexShardStage.values());
        SnapshotStats stats = new SnapshotStatsTests().createTestInstance();
        String nodeId = randomAlphaOfLength(20);
        String failure = null;
        if (rarely()) {
            failure = randomAlphaOfLength(200);
        }
        return new SnapshotIndexShardStatus(shardId, stage, stats, nodeId, failure);
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // Do not place random fields in the root object since its fields correspond to shard names.
        return String::isEmpty;
    }

    @Override
    protected SnapshotIndexShardStatus doParseInstance(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser);
        SnapshotIndexShardStatus status = SnapshotIndexShardStatus.fromXContent(parser, parser.currentName());
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
        return status;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
