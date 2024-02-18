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

package org.opensearch.search;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchHit.NestedIdentity;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.opensearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;

public class NestedIdentityTests extends OpenSearchTestCase {

    public static NestedIdentity createTestItem(int depth) {
        String field = frequently() ? randomAlphaOfLengthBetween(1, 20) : randomRealisticUnicodeOfCodepointLengthBetween(1, 20);
        int offset = randomInt(10);
        NestedIdentity child = null;
        if (depth > 0) {
            child = createTestItem(depth - 1);
        }
        return new NestedIdentity(field, offset, child);
    }

    public void testFromXContent() throws IOException {
        NestedIdentity nestedIdentity = createTestItem(randomInt(3));
        XContentType xcontentType = randomFrom(XContentType.values());
        XContentBuilder builder = MediaTypeRegistry.contentBuilder(xcontentType);
        if (randomBoolean()) {
            builder.prettyPrint();
        }
        builder = nestedIdentity.innerToXContent(builder, ToXContent.EMPTY_PARAMS);
        try (XContentParser parser = createParser(builder)) {
            NestedIdentity parsedNestedIdentity = NestedIdentity.fromXContent(parser);
            assertEquals(nestedIdentity, parsedNestedIdentity);
            assertNull(parser.nextToken());
        }
    }

    public void testToXContent() throws IOException {
        NestedIdentity nestedIdentity = new NestedIdentity("foo", 5, null);
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.prettyPrint();
        builder.startObject();
        nestedIdentity.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals(
            "{\n" + "  \"_nested\" : {\n" + "    \"field\" : \"foo\",\n" + "    \"offset\" : 5\n" + "  }\n" + "}",
            builder.toString()
        );

        nestedIdentity = new NestedIdentity("foo", 5, new NestedIdentity("bar", 3, null));
        builder = JsonXContent.contentBuilder();
        builder.prettyPrint();
        builder.startObject();
        nestedIdentity.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals(
            "{\n"
                + "  \"_nested\" : {\n"
                + "    \"field\" : \"foo\",\n"
                + "    \"offset\" : 5,\n"
                + "    \"_nested\" : {\n"
                + "      \"field\" : \"bar\",\n"
                + "      \"offset\" : 3\n"
                + "    }\n"
                + "  }\n"
                + "}",
            builder.toString()
        );
    }

    /**
     * Test equality and hashCode properties
     */
    public void testEqualsAndHashcode() {
        checkEqualsAndHashCode(createTestItem(randomInt(3)), NestedIdentityTests::copy, NestedIdentityTests::mutate);
    }

    public void testSerialization() throws IOException {
        NestedIdentity nestedIdentity = createTestItem(randomInt(3));
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            nestedIdentity.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                NestedIdentity deserializedCopy = new NestedIdentity(in);
                assertEquals(nestedIdentity, deserializedCopy);
                assertEquals(nestedIdentity.hashCode(), deserializedCopy.hashCode());
                assertNotSame(nestedIdentity, deserializedCopy);
            }
        }
    }

    private static NestedIdentity mutate(NestedIdentity original) {
        if (original == null) {
            return createTestItem(0);
        }
        List<Supplier<NestedIdentity>> mutations = new ArrayList<>();
        int offset = original.getOffset();
        NestedIdentity child = (NestedIdentity) original.getChild();
        String fieldName = original.getField().string();
        mutations.add(() -> new NestedIdentity(original.getField().string() + "_prefix", offset, child));
        mutations.add(() -> new NestedIdentity(fieldName, offset + 1, child));
        mutations.add(() -> new NestedIdentity(fieldName, offset, mutate(child)));
        return randomFrom(mutations).get();
    }

    private static NestedIdentity copy(NestedIdentity original) {
        NestedIdentity child = original.getChild();
        return new NestedIdentity(original.getField().string(), original.getOffset(), child != null ? copy(child) : null);
    }

}
