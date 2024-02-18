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

package org.opensearch.search.profile;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.opensearch.core.xcontent.XContentHelper.toXContent;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.test.XContentTestUtils.insertRandomFields;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertToXContentEquivalent;

public class ProfileResultTests extends OpenSearchTestCase {

    public static ProfileResult createTestItem(int depth, boolean concurrentSegmentSearchEnabled) {
        String type = randomAlphaOfLengthBetween(5, 10);
        String description = randomAlphaOfLengthBetween(5, 10);
        int breakdownsSize = randomIntBetween(0, 5);
        Map<String, Long> breakdown = new HashMap<>(breakdownsSize);
        while (breakdown.size() < breakdownsSize) {
            long value = randomNonNegativeLong();
            if (randomBoolean()) {
                // also often use "small" values in tests
                value = value % 10000;
            }
            breakdown.put(randomAlphaOfLengthBetween(5, 10), value);
        }
        int debugSize = randomIntBetween(0, 5);
        Map<String, Object> debug = new HashMap<>(debugSize);
        while (debug.size() < debugSize) {
            debug.put(randomAlphaOfLength(5), randomAlphaOfLength(4));
        }
        int childrenSize = depth > 0 ? randomIntBetween(0, 1) : 0;
        List<ProfileResult> children = new ArrayList<>(childrenSize);
        for (int i = 0; i < childrenSize; i++) {
            children.add(createTestItem(depth - 1, concurrentSegmentSearchEnabled));
        }
        if (concurrentSegmentSearchEnabled) {
            return new ProfileResult(
                type,
                description,
                breakdown,
                debug,
                randomNonNegativeLong(),
                children,
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong()
            );
        } else {
            return new ProfileResult(type, description, breakdown, debug, randomNonNegativeLong(), children);
        }
    }

    public void testFromXContent() throws IOException {
        doFromXContentTestWithRandomFields(false, false);
        doFromXContentTestWithRandomFields(false, true);
    }

    /**
     * This test adds random fields and objects to the xContent rendered out to ensure we can parse it
     * back to be forward compatible with additions to the xContent
     */
    public void testFromXContentWithRandomFields() throws IOException {
        doFromXContentTestWithRandomFields(true, false);
        doFromXContentTestWithRandomFields(true, true);
    }

    private void doFromXContentTestWithRandomFields(boolean addRandomFields, boolean concurrentSegmentSearchEnabled) throws IOException {
        ProfileResult profileResult = createTestItem(2, concurrentSegmentSearchEnabled);
        XContentType xContentType = randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toShuffledXContent(profileResult, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
        BytesReference mutated;
        if (addRandomFields) {
            // "breakdown" and "debug" just consists of key/value pairs, we shouldn't add anything random there
            Predicate<String> excludeFilter = (s) -> s.endsWith(ProfileResult.BREAKDOWN.getPreferredName())
                || s.endsWith(ProfileResult.DEBUG.getPreferredName());
            mutated = insertRandomFields(xContentType, originalBytes, excludeFilter, random());
        } else {
            mutated = originalBytes;
        }
        ProfileResult parsed;
        try (XContentParser parser = createParser(xContentType.xContent(), mutated)) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            parsed = ProfileResult.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }
        assertEquals(profileResult.getTime(), parsed.getTime());
        assertEquals(profileResult.getMaxSliceTime(), parsed.getMaxSliceTime());
        assertEquals(profileResult.getMinSliceTime(), parsed.getMinSliceTime());
        assertEquals(profileResult.getAvgSliceTime(), parsed.getAvgSliceTime());
        assertToXContentEquivalent(originalBytes, toXContent(parsed, xContentType, humanReadable), xContentType);
    }

    public void testToXContent() throws IOException {
        List<ProfileResult> children = new ArrayList<>();
        children.add(new ProfileResult("child1", "desc1", Map.of("key1", 100L), Map.of(), 100L, List.of()));
        children.add(new ProfileResult("child2", "desc2", Map.of("key1", 123356L), Map.of(), 123356L, List.of()));
        Map<String, Long> breakdown = new LinkedHashMap<>();
        breakdown.put("key1", 123456L);
        breakdown.put("stuff", 10000L);
        Map<String, Object> debug = new LinkedHashMap<>();
        debug.put("a", "foo");
        debug.put("b", "bar");
        ProfileResult result = new ProfileResult("someType", "some description", breakdown, debug, 223456L, children);
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals(
            "{\n"
                + "  \"type\" : \"someType\",\n"
                + "  \"description\" : \"some description\",\n"
                + "  \"time_in_nanos\" : 223456,\n"
                + "  \"breakdown\" : {\n"
                + "    \"key1\" : 123456,\n"
                + "    \"stuff\" : 10000\n"
                + "  },\n"
                + "  \"debug\" : {\n"
                + "    \"a\" : \"foo\",\n"
                + "    \"b\" : \"bar\"\n"
                + "  },\n"
                + "  \"children\" : [\n"
                + "    {\n"
                + "      \"type\" : \"child1\",\n"
                + "      \"description\" : \"desc1\",\n"
                + "      \"time_in_nanos\" : 100,\n"
                + "      \"breakdown\" : {\n"
                + "        \"key1\" : 100\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"type\" : \"child2\",\n"
                + "      \"description\" : \"desc2\",\n"
                + "      \"time_in_nanos\" : 123356,\n"
                + "      \"breakdown\" : {\n"
                + "        \"key1\" : 123356\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}",
            builder.toString()
        );

        builder = XContentFactory.jsonBuilder().prettyPrint().humanReadable(true);
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals(
            "{\n"
                + "  \"type\" : \"someType\",\n"
                + "  \"description\" : \"some description\",\n"
                + "  \"time\" : \"223.4micros\",\n"
                + "  \"time_in_nanos\" : 223456,\n"
                + "  \"breakdown\" : {\n"
                + "    \"key1\" : 123456,\n"
                + "    \"stuff\" : 10000\n"
                + "  },\n"
                + "  \"debug\" : {\n"
                + "    \"a\" : \"foo\",\n"
                + "    \"b\" : \"bar\"\n"
                + "  },\n"
                + "  \"children\" : [\n"
                + "    {\n"
                + "      \"type\" : \"child1\",\n"
                + "      \"description\" : \"desc1\",\n"
                + "      \"time\" : \"100nanos\",\n"
                + "      \"time_in_nanos\" : 100,\n"
                + "      \"breakdown\" : {\n"
                + "        \"key1\" : 100\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"type\" : \"child2\",\n"
                + "      \"description\" : \"desc2\",\n"
                + "      \"time\" : \"123.3micros\",\n"
                + "      \"time_in_nanos\" : 123356,\n"
                + "      \"breakdown\" : {\n"
                + "        \"key1\" : 123356\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}",
            builder.toString()
        );

        result = new ProfileResult("profileName", "some description", Map.of("key1", 12345678L), Map.of(), 12345678L, List.of());
        builder = XContentFactory.jsonBuilder().prettyPrint().humanReadable(true);
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals(
            "{\n"
                + "  \"type\" : \"profileName\",\n"
                + "  \"description\" : \"some description\",\n"
                + "  \"time\" : \"12.3ms\",\n"
                + "  \"time_in_nanos\" : 12345678,\n"
                + "  \"breakdown\" : {\n"
                + "    \"key1\" : 12345678\n"
                + "  }\n"
                + "}",
            builder.toString()
        );

        result = new ProfileResult("profileName", "some description", Map.of("key1", 1234567890L), Map.of(), 1234567890L, List.of());
        builder = XContentFactory.jsonBuilder().prettyPrint().humanReadable(true);
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals(
            "{\n"
                + "  \"type\" : \"profileName\",\n"
                + "  \"description\" : \"some description\",\n"
                + "  \"time\" : \"1.2s\",\n"
                + "  \"time_in_nanos\" : 1234567890,\n"
                + "  \"breakdown\" : {\n"
                + "    \"key1\" : 1234567890\n"
                + "  }\n"
                + "}",
            builder.toString()
        );

        result = new ProfileResult("profileName", "some description", Map.of("key1", 1234L), Map.of(), 1234L, List.of(), 321L, 123L, 222L);
        builder = XContentFactory.jsonBuilder().prettyPrint();
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals(
            "{\n"
                + "  \"type\" : \"profileName\",\n"
                + "  \"description\" : \"some description\",\n"
                + "  \"time_in_nanos\" : 1234,\n"
                + "  \"max_slice_time_in_nanos\" : 321,\n"
                + "  \"min_slice_time_in_nanos\" : 123,\n"
                + "  \"avg_slice_time_in_nanos\" : 222,\n"
                + "  \"breakdown\" : {\n"
                + "    \"key1\" : 1234\n"
                + "  }\n"
                + "}",
            builder.toString()
        );

        result = new ProfileResult(
            "profileName",
            "some description",
            Map.of("key1", 1234567890L),
            Map.of(),
            1234567890L,
            List.of(),
            87654321L,
            12345678L,
            54637281L
        );
        builder = XContentFactory.jsonBuilder().prettyPrint().humanReadable(true);
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals(
            "{\n"
                + "  \"type\" : \"profileName\",\n"
                + "  \"description\" : \"some description\",\n"
                + "  \"time\" : \"1.2s\",\n"
                + "  \"max_slice_time\" : \"87.6ms\",\n"
                + "  \"min_slice_time\" : \"12.3ms\",\n"
                + "  \"avg_slice_time\" : \"54.6ms\",\n"
                + "  \"time_in_nanos\" : 1234567890,\n"
                + "  \"max_slice_time_in_nanos\" : 87654321,\n"
                + "  \"min_slice_time_in_nanos\" : 12345678,\n"
                + "  \"avg_slice_time_in_nanos\" : 54637281,\n"
                + "  \"breakdown\" : {\n"
                + "    \"key1\" : 1234567890\n"
                + "  }\n"
                + "}",
            builder.toString()
        );

    }

    public void testRemoveStartTimeFields() {
        Map<String, Long> breakdown = new HashMap<>();
        breakdown.put("initialize_start_time", 123456L);
        breakdown.put("initialize_count", 1L);
        breakdown.put("initialize", 654321L);
        Map<String, Long> modifiedBreakdown = new LinkedHashMap<>(breakdown);
        assertEquals(3, modifiedBreakdown.size());
        assertEquals(123456L, (long) modifiedBreakdown.get("initialize_start_time"));
        assertEquals(1L, (long) modifiedBreakdown.get("initialize_count"));
        assertEquals(654321L, (long) modifiedBreakdown.get("initialize"));
        ProfileResult.removeStartTimeFields(modifiedBreakdown);
        assertFalse(modifiedBreakdown.containsKey("initialize_start_time"));
        assertTrue(modifiedBreakdown.containsKey("initialize_count"));
        assertTrue(modifiedBreakdown.containsKey("initialize"));
    }
}
