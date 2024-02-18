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

package org.opensearch.action;

import org.opensearch.action.DocWriteResponse.Result;
import org.opensearch.action.support.replication.ReplicationResponse.ShardInfo;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class DocWriteResponseTests extends OpenSearchTestCase {
    public void testGetLocation() {
        final DocWriteResponse response = new DocWriteResponse(
            new ShardId("index", "uuid", 0),
            "id",
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            17,
            0,
            Result.CREATED
        ) {
        };
        assertEquals("/index/" + MapperService.SINGLE_MAPPING_NAME + "/id", response.getLocation(null));
        assertEquals("/index/" + MapperService.SINGLE_MAPPING_NAME + "/id?routing=test_routing", response.getLocation("test_routing"));
    }

    public void testGetLocationNonAscii() {
        final DocWriteResponse response = new DocWriteResponse(
            new ShardId("index", "uuid", 0),
            "❤",
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            17,
            0,
            Result.CREATED
        ) {
        };
        assertEquals("/index/" + MapperService.SINGLE_MAPPING_NAME + "/%E2%9D%A4", response.getLocation(null));
        assertEquals("/index/" + MapperService.SINGLE_MAPPING_NAME + "/%E2%9D%A4?routing=%C3%A4", response.getLocation("ä"));
    }

    public void testGetLocationWithSpaces() {
        final DocWriteResponse response = new DocWriteResponse(
            new ShardId("index", "uuid", 0),
            "a b",
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            17,
            0,
            Result.CREATED
        ) {
        };
        assertEquals("/index/" + MapperService.SINGLE_MAPPING_NAME + "/a+b", response.getLocation(null));
        assertEquals("/index/" + MapperService.SINGLE_MAPPING_NAME + "/a+b?routing=c+d", response.getLocation("c d"));
    }

    /**
     * Tests that {@link DocWriteResponse#toXContent(XContentBuilder, ToXContent.Params)} doesn't include {@code forced_refresh} unless it
     * is true. We can't assert this in the yaml tests because "not found" is also "false" there....
     */
    public void testToXContentDoesntIncludeForcedRefreshUnlessForced() throws IOException {
        DocWriteResponse response = new DocWriteResponse(
            new ShardId("index", "uuid", 0),
            "id",
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            17,
            0,
            Result.CREATED
        ) {
            // DocWriteResponse is abstract so we have to sneak a subclass in here to test it.
        };
        response.setShardInfo(new ShardInfo(1, 1));
        response.setForcedRefresh(false);
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
                assertThat(parser.map(), not(hasKey("forced_refresh")));
            }
        }
        response.setForcedRefresh(true);
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
                assertThat(parser.map(), hasEntry("forced_refresh", true));
            }
        }
    }
}
