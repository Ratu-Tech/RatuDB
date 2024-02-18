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
 * the Apache License, Version 2.0 (the \"License\"); you may
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

package org.opensearch.search.profile.aggregation;

import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParserUtils;
import org.opensearch.search.profile.ProfileResult;
import org.opensearch.search.profile.ProfileResultTests;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.core.xcontent.XContentHelper.toXContent;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertToXContentEquivalent;

public class AggregationProfileShardResultTests extends OpenSearchTestCase {

    public static AggregationProfileShardResult createTestItem(int depth) {
        int size = randomIntBetween(0, 5);
        List<ProfileResult> aggProfileResults = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            aggProfileResults.add(ProfileResultTests.createTestItem(depth, false));
        }
        return new AggregationProfileShardResult(aggProfileResults);
    }

    public void testFromXContent() throws IOException {
        AggregationProfileShardResult profileResult = createTestItem(2);
        XContentType xContentType = randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toShuffledXContent(profileResult, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);

        AggregationProfileShardResult parsed;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            XContentParserUtils.ensureFieldName(parser, parser.nextToken(), AggregationProfileShardResult.AGGREGATIONS);
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.nextToken(), parser);
            parsed = AggregationProfileShardResult.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, xContentType, humanReadable), xContentType);
    }

    public void testToXContent() throws IOException {
        List<ProfileResult> profileResults = new ArrayList<>();
        Map<String, Long> breakdown = new LinkedHashMap<>();
        breakdown.put("timing1", 2000L);
        breakdown.put("timing2", 4000L);
        Map<String, Object> debug = new LinkedHashMap<>();
        debug.put("stuff", "stuff");
        debug.put("other_stuff", List.of("foo", "bar"));
        ProfileResult profileResult = new ProfileResult("someType", "someDescription", breakdown, debug, 6000L, Collections.emptyList());
        profileResults.add(profileResult);
        AggregationProfileShardResult aggProfileResults = new AggregationProfileShardResult(profileResults);
        BytesReference xContent = toXContent(aggProfileResults, MediaTypeRegistry.JSON, false);
        assertEquals(
            "{\"aggregations\":["
                + "{\"type\":\"someType\","
                + "\"description\":\"someDescription\","
                + "\"time_in_nanos\":6000,"
                + "\"breakdown\":{\"timing1\":2000,\"timing2\":4000},"
                + "\"debug\":{\"stuff\":\"stuff\",\"other_stuff\":[\"foo\",\"bar\"]}"
                + "}"
                + "]}",
            xContent.utf8ToString()
        );

        xContent = toXContent(aggProfileResults, MediaTypeRegistry.JSON, true);
        assertEquals(
            "{\"aggregations\":["
                + "{\"type\":\"someType\","
                + "\"description\":\"someDescription\","
                + "\"time\":\"6micros\","
                + "\"time_in_nanos\":6000,"
                + "\"breakdown\":{\"timing1\":2000,\"timing2\":4000},"
                + "\"debug\":{\"stuff\":\"stuff\",\"other_stuff\":[\"foo\",\"bar\"]}"
                + "}"
                + "]}",
            xContent.utf8ToString()
        );
    }

}
