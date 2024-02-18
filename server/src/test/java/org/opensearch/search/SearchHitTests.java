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

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TotalHits;
import org.opensearch.Version;
import org.opensearch.action.OriginalIndices;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.get.GetResultTests;
import org.opensearch.search.SearchHit.NestedIdentity;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;
import org.opensearch.search.fetch.subphase.highlight.HighlightFieldTests;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.test.RandomObjects;
import org.opensearch.test.VersionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.opensearch.core.xcontent.XContentHelper.toXContent;
import static org.opensearch.test.XContentTestUtils.insertRandomFields;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class SearchHitTests extends AbstractWireSerializingTestCase<SearchHit> {
    public static SearchHit createTestItem(boolean withOptionalInnerHits, boolean withShardTarget) {
        return createTestItem(randomFrom(XContentType.values()), withOptionalInnerHits, withShardTarget);
    }

    public static SearchHit createTestItem(final MediaType mediaType, boolean withOptionalInnerHits, boolean transportSerialization) {
        int internalId = randomInt();
        String uid = randomAlphaOfLength(10);
        NestedIdentity nestedIdentity = null;
        if (randomBoolean()) {
            nestedIdentity = NestedIdentityTests.createTestItem(randomIntBetween(0, 2));
        }
        Map<String, DocumentField> metaFields = new HashMap<>();
        Map<String, DocumentField> documentFields = new HashMap<>();
        if (frequently()) {
            if (randomBoolean()) {
                metaFields = GetResultTests.randomDocumentFields(mediaType, true).v2();
                documentFields = GetResultTests.randomDocumentFields(mediaType, false).v2();
            }
        }

        SearchHit hit = new SearchHit(internalId, uid, nestedIdentity, documentFields, metaFields);
        if (frequently()) {
            if (rarely()) {
                hit.score(Float.NaN);
            } else {
                hit.score(randomFloat());
            }
        }
        if (frequently()) {
            hit.sourceRef(RandomObjects.randomSource(random(), mediaType));
        }
        if (randomBoolean()) {
            hit.version(randomLong());
        }

        if (randomBoolean()) {
            hit.version(randomNonNegativeLong());
            hit.version(randomLongBetween(1, Long.MAX_VALUE));
        }
        if (randomBoolean()) {
            hit.sortValues(SearchSortValuesTests.createTestItem(mediaType, transportSerialization));
        }
        if (randomBoolean()) {
            int size = randomIntBetween(0, 5);
            Map<String, HighlightField> highlightFields = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                HighlightField testItem = HighlightFieldTests.createTestItem();
                highlightFields.put(testItem.getName(), testItem);
            }
            hit.highlightFields(highlightFields);
        }
        if (randomBoolean()) {
            int size = randomIntBetween(0, 5);
            String[] matchedQueries = new String[size];
            for (int i = 0; i < size; i++) {
                matchedQueries[i] = randomAlphaOfLength(5);
            }
            hit.matchedQueries(matchedQueries);
        }
        if (randomBoolean()) {
            hit.explanation(createExplanation(randomIntBetween(0, 5)));
        }
        if (withOptionalInnerHits) {
            int innerHitsSize = randomIntBetween(0, 3);
            if (innerHitsSize > 0) {
                Map<String, SearchHits> innerHits = new HashMap<>(innerHitsSize);
                for (int i = 0; i < innerHitsSize; i++) {
                    innerHits.put(randomAlphaOfLength(5), SearchHitsTests.createTestItem(mediaType, false, transportSerialization));
                }
                hit.setInnerHits(innerHits);
            }
        }
        if (transportSerialization && randomBoolean()) {
            String index = randomAlphaOfLengthBetween(5, 10);
            String clusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
            hit.shard(
                new SearchShardTarget(
                    randomAlphaOfLengthBetween(5, 10),
                    new ShardId(new Index(index, randomAlphaOfLengthBetween(5, 10)), randomInt()),
                    clusterAlias,
                    OriginalIndices.NONE
                )
            );
        }
        return hit;
    }

    @Override
    protected Writeable.Reader<SearchHit> instanceReader() {
        return SearchHit::new;
    }

    @Override
    protected SearchHit createTestInstance() {
        return createTestItem(randomFrom(XContentType.values()), randomBoolean(), randomBoolean());
    }

    public void testFromXContent() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        SearchHit searchHit = createTestItem(xContentType, true, false);
        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toShuffledXContent(searchHit, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
        SearchHit parsed;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            parser.nextToken(); // jump to first START_OBJECT
            parsed = SearchHit.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, xContentType, humanReadable), xContentType);
    }

    /**
     * This test adds randomized fields on all json objects and checks that we can parse it to
     * ensure the parsing is lenient for forward compatibility.
     * We need to exclude json objects with the "highlight" and "fields" field name since these
     * objects allow arbitrary keys (the field names that are queries). Also we want to exclude
     * to add anything under "_source" since it is not parsed, and avoid complexity by excluding
     * everything under "inner_hits". They are also keyed by arbitrary names and contain SearchHits,
     * which are already tested elsewhere. We also exclude the root level, as all unknown fields
     * on a root level are interpreted as meta-fields and will be kept.
     */
    public void testFromXContentLenientParsing() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        SearchHit searchHit = createTestItem(xContentType, true, true);
        BytesReference originalBytes = toXContent(searchHit, xContentType, true);
        Predicate<String> pathsToExclude = path -> path.endsWith("highlight")
            || path.contains("fields")
            || path.contains("_source")
            || path.contains("inner_hits")
            || path.isEmpty();
        BytesReference withRandomFields = insertRandomFields(xContentType, originalBytes, pathsToExclude, random());

        SearchHit parsed;
        try (XContentParser parser = createParser(xContentType.xContent(), withRandomFields)) {
            parser.nextToken(); // jump to first START_OBJECT
            parsed = SearchHit.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, xContentType, true), xContentType);
    }

    /**
     * When e.g. with "stored_fields": "_none_", only "_index" and "_score" are returned.
     */
    public void testFromXContentWithoutTypeAndId() throws IOException {
        String hit = "{\"_index\": \"my_index\", \"_score\": 1}";
        SearchHit parsed;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, hit)) {
            parser.nextToken(); // jump to first START_OBJECT
            parsed = SearchHit.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }
        assertEquals("my_index", parsed.getIndex());
        assertEquals(1, parsed.getScore(), Float.MIN_VALUE);
        assertNull(parsed.getId());
    }

    public void testToXContent() throws IOException {
        SearchHit searchHit = new SearchHit(1, "id1", Collections.emptyMap(), Collections.emptyMap());
        searchHit.score(1.5f);
        XContentBuilder builder = JsonXContent.contentBuilder();
        searchHit.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\"_id\":\"id1\",\"_score\":1.5}", builder.toString());
    }

    public void testSerializeShardTarget() throws Exception {
        String clusterAlias = randomBoolean() ? null : "cluster_alias";
        SearchShardTarget target = new SearchShardTarget(
            "_node_id",
            new ShardId(new Index("_index", "_na_"), 0),
            clusterAlias,
            OriginalIndices.NONE
        );

        Map<String, SearchHits> innerHits = new HashMap<>();
        SearchHit innerHit1 = new SearchHit(0, "_id", null, null);
        innerHit1.shard(target);
        SearchHit innerInnerHit2 = new SearchHit(0, "_id", null, null);
        innerInnerHit2.shard(target);
        innerHits.put("1", new SearchHits(new SearchHit[] { innerInnerHit2 }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1f));
        innerHit1.setInnerHits(innerHits);
        SearchHit innerHit2 = new SearchHit(0, "_id", null, null);
        innerHit2.shard(target);
        SearchHit innerHit3 = new SearchHit(0, "_id", null, null);
        innerHit3.shard(target);

        innerHits = new HashMap<>();
        SearchHit hit1 = new SearchHit(0, "_id", null, null);
        innerHits.put("1", new SearchHits(new SearchHit[] { innerHit1, innerHit2 }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1f));
        innerHits.put("2", new SearchHits(new SearchHit[] { innerHit3 }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1f));
        hit1.shard(target);
        hit1.setInnerHits(innerHits);

        SearchHit hit2 = new SearchHit(0, "_id", null, null);
        hit2.shard(target);

        SearchHits hits = new SearchHits(new SearchHit[] { hit1, hit2 }, new TotalHits(2, TotalHits.Relation.EQUAL_TO), 1f);

        Version version = VersionUtils.randomVersion(random());
        SearchHits results = copyWriteable(hits, getNamedWriteableRegistry(), SearchHits::new, version);
        SearchShardTarget deserializedTarget = results.getAt(0).getShard();
        assertThat(deserializedTarget, equalTo(target));
        assertThat(results.getAt(0).getInnerHits().get("1").getAt(0).getShard(), notNullValue());
        assertThat(results.getAt(0).getInnerHits().get("1").getAt(0).getInnerHits().get("1").getAt(0).getShard(), notNullValue());
        assertThat(results.getAt(0).getInnerHits().get("1").getAt(1).getShard(), notNullValue());
        assertThat(results.getAt(0).getInnerHits().get("2").getAt(0).getShard(), notNullValue());
        for (SearchHit hit : results) {
            assertEquals(clusterAlias, hit.getClusterAlias());
            if (hit.getInnerHits() != null) {
                for (SearchHits innerhits : hit.getInnerHits().values()) {
                    for (SearchHit innerHit : innerhits) {
                        assertEquals(clusterAlias, innerHit.getClusterAlias());
                    }
                }
            }
        }
        assertThat(results.getAt(1).getShard(), equalTo(target));
    }

    public void testNullSource() {
        SearchHit searchHit = new SearchHit(0, "_id", null, null);

        assertThat(searchHit.getSourceAsMap(), nullValue());
        assertThat(searchHit.getSourceRef(), nullValue());
        assertThat(searchHit.getSourceAsMap(), nullValue());
        assertThat(searchHit.getSourceAsString(), nullValue());
        assertThat(searchHit.getSourceAsMap(), nullValue());
        assertThat(searchHit.getSourceRef(), nullValue());
        assertThat(searchHit.getSourceAsString(), nullValue());
    }

    public void testHasSource() {
        SearchHit searchHit = new SearchHit(randomInt());
        assertFalse(searchHit.hasSource());
        searchHit.sourceRef(new BytesArray("{}"));
        assertTrue(searchHit.hasSource());
    }

    public void testWeirdScriptFields() throws Exception {
        {
            XContentParser parser = createParser(
                MediaTypeRegistry.JSON.xContent(),
                "{\n"
                    + "  \"_index\": \"twitter\",\n"
                    + "  \"_id\": \"1\",\n"
                    + "  \"_score\": 1.0,\n"
                    + "  \"fields\": {\n"
                    + "    \"result\": [null]\n"
                    + "  }\n"
                    + "}"
            );
            SearchHit searchHit = SearchHit.fromXContent(parser);
            Map<String, DocumentField> fields = searchHit.getFields();
            assertEquals(1, fields.size());
            DocumentField result = fields.get("result");
            assertNotNull(result);
            assertEquals(1, result.getValues().size());
            assertNull(result.getValues().get(0));
        }
        {
            XContentParser parser = createParser(
                MediaTypeRegistry.JSON.xContent(),
                "{\n"
                    + "  \"_index\": \"twitter\",\n"
                    + "  \"_id\": \"1\",\n"
                    + "  \"_score\": 1.0,\n"
                    + "  \"fields\": {\n"
                    + "    \"result\": [{}]\n"
                    + "  }\n"
                    + "}"
            );

            SearchHit searchHit = SearchHit.fromXContent(parser);
            Map<String, DocumentField> fields = searchHit.getFields();
            assertEquals(1, fields.size());
            DocumentField result = fields.get("result");
            assertNotNull(result);
            assertEquals(1, result.getValues().size());
            Object value = result.getValues().get(0);
            assertThat(value, instanceOf(Map.class));
            Map<?, ?> map = (Map<?, ?>) value;
            assertEquals(0, map.size());
        }
        {
            XContentParser parser = createParser(
                JsonXContent.jsonXContent,
                "{\n"
                    + "  \"_index\": \"twitter\",\n"
                    + "  \"_id\": \"1\",\n"
                    + "  \"_score\": 1.0,\n"
                    + "  \"fields\": {\n"
                    + "    \"result\": [\n"
                    + "      []\n"
                    + "    ]\n"
                    + "  }\n"
                    + "}"
            );

            SearchHit searchHit = SearchHit.fromXContent(parser);
            Map<String, DocumentField> fields = searchHit.getFields();
            assertEquals(1, fields.size());
            DocumentField result = fields.get("result");
            assertNotNull(result);
            assertEquals(1, result.getValues().size());
            Object value = result.getValues().get(0);
            assertThat(value, instanceOf(List.class));
            List<?> list = (List<?>) value;
            assertEquals(0, list.size());
        }
    }

    public void testToXContentEmptyFields() throws IOException {
        Map<String, DocumentField> fields = new HashMap<>();
        fields.put("foo", new DocumentField("foo", Collections.emptyList()));
        fields.put("bar", new DocumentField("bar", Collections.emptyList()));
        SearchHit hit = new SearchHit(0, "_id", null, fields, Collections.emptyMap());
        {
            BytesReference originalBytes = toShuffledXContent(hit, MediaTypeRegistry.JSON, ToXContent.EMPTY_PARAMS, randomBoolean());
            // checks that the fields section is completely omitted in the rendering.
            assertThat(originalBytes.utf8ToString(), not(containsString("fields")));
            final SearchHit parsed;
            try (XContentParser parser = createParser(MediaTypeRegistry.JSON.xContent(), originalBytes)) {
                parser.nextToken(); // jump to first START_OBJECT
                parsed = SearchHit.fromXContent(parser);
                assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
                assertNull(parser.nextToken());
            }
            assertThat(parsed.getFields().size(), equalTo(0));
        }

        fields = new HashMap<>();
        fields.put("foo", new DocumentField("foo", Collections.emptyList()));
        fields.put("bar", new DocumentField("bar", Collections.singletonList("value")));
        hit = new SearchHit(0, "_id", null, fields, Collections.emptyMap());
        {
            BytesReference originalBytes = toShuffledXContent(hit, MediaTypeRegistry.JSON, ToXContent.EMPTY_PARAMS, randomBoolean());
            final SearchHit parsed;
            try (XContentParser parser = createParser(MediaTypeRegistry.JSON.xContent(), originalBytes)) {
                parser.nextToken(); // jump to first START_OBJECT
                parsed = SearchHit.fromXContent(parser);
                assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
                assertNull(parser.nextToken());
            }
            assertThat(parsed.getFields().size(), equalTo(1));
            assertThat(parsed.getFields().get("bar").getValues(), equalTo(Collections.singletonList("value")));
        }

        Map<String, DocumentField> metadata = new HashMap<>();
        metadata.put("_routing", new DocumentField("_routing", Collections.emptyList()));
        hit = new SearchHit(0, "_id", null, fields, Collections.emptyMap());
        {
            BytesReference originalBytes = toShuffledXContent(hit, MediaTypeRegistry.JSON, ToXContent.EMPTY_PARAMS, randomBoolean());
            final SearchHit parsed;
            try (XContentParser parser = createParser(MediaTypeRegistry.JSON.xContent(), originalBytes)) {
                parser.nextToken(); // jump to first START_OBJECT
                parsed = SearchHit.fromXContent(parser);
                assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
                assertNull(parser.nextToken());
            }
            assertThat(parsed.getFields().size(), equalTo(1));
            assertThat(parsed.getFields().get("bar").getValues(), equalTo(Collections.singletonList("value")));
            assertNull(parsed.getFields().get("_routing"));
        }

    }

    static Explanation createExplanation(int depth) {
        String description = randomAlphaOfLengthBetween(5, 20);
        float value = randomFloat();
        List<Explanation> details = new ArrayList<>();
        if (depth > 0) {
            int numberOfDetails = randomIntBetween(1, 3);
            for (int i = 0; i < numberOfDetails; i++) {
                details.add(createExplanation(depth - 1));
            }
        }
        return Explanation.match(value, description, details);
    }
}
