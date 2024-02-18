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

package org.opensearch.percolator;

import org.apache.lucene.search.Query;
import org.opensearch.OpenSearchException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.Rewriteable;
import org.opensearch.ingest.RandomDocumentPicks;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.AbstractQueryTestCase;
import org.opensearch.test.TestGeoShapeFieldMapperPlugin;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PercolateQueryBuilderTests extends AbstractQueryTestCase<PercolateQueryBuilder> {

    private static final String[] SHUFFLE_PROTECTED_FIELDS = new String[] {
        PercolateQueryBuilder.DOCUMENT_FIELD.getPreferredName(),
        PercolateQueryBuilder.DOCUMENTS_FIELD.getPreferredName() };

    protected static String queryField = "field";
    protected static String aliasField = "alias";
    private static String docType;

    private String indexedDocumentIndex;
    private String indexedDocumentId;
    private String indexedDocumentRouting;
    private String indexedDocumentPreference;
    private Long indexedDocumentVersion;
    private List<BytesReference> documentSource;

    private boolean indexedDocumentExists = true;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(PercolatorModulePlugin.class, TestGeoShapeFieldMapperPlugin.class);
    }

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        queryField = randomAlphaOfLength(4);
        aliasField = randomAlphaOfLength(4);

        docType = "_doc";
        mapperService.merge(
            docType,
            new CompressedXContent(
                PutMappingRequest.simpleMapping(queryField, "type=percolator", aliasField, "type=alias,path=" + queryField).toString()
            ),
            MapperService.MergeReason.MAPPING_UPDATE
        );
        mapperService.merge(
            docType,
            new CompressedXContent(PutMappingRequest.simpleMapping(TEXT_FIELD_NAME, "type=text").toString()),
            MapperService.MergeReason.MAPPING_UPDATE
        );
    }

    @Override
    protected PercolateQueryBuilder doCreateTestQueryBuilder() {
        return doCreateTestQueryBuilder(randomBoolean());
    }

    private PercolateQueryBuilder doCreateTestQueryBuilder(boolean indexedDocument) {
        if (indexedDocument) {
            documentSource = Collections.singletonList(randomSource(new HashSet<>()));
        } else {
            int numDocs = randomIntBetween(1, 8);
            documentSource = new ArrayList<>(numDocs);
            Set<String> usedFields = new HashSet<>();
            for (int i = 0; i < numDocs; i++) {
                documentSource.add(randomSource(usedFields));
            }
        }

        PercolateQueryBuilder queryBuilder;
        if (indexedDocument) {
            indexedDocumentIndex = randomAlphaOfLength(4);
            indexedDocumentId = randomAlphaOfLength(4);
            indexedDocumentRouting = randomAlphaOfLength(4);
            indexedDocumentPreference = randomAlphaOfLength(4);
            indexedDocumentVersion = (long) randomIntBetween(0, Integer.MAX_VALUE);
            queryBuilder = new PercolateQueryBuilder(
                queryField,
                indexedDocumentIndex,
                indexedDocumentId,
                indexedDocumentRouting,
                indexedDocumentPreference,
                indexedDocumentVersion
            );
        } else {
            queryBuilder = new PercolateQueryBuilder(queryField, documentSource, MediaTypeRegistry.JSON);
        }
        if (randomBoolean()) {
            queryBuilder.setName(randomAlphaOfLength(4));
        }
        return queryBuilder;
    }

    /**
     * we don't want to shuffle the "document" field internally in {@link #testFromXContent()} because even though the
     * documents would be functionally the same, their {@link BytesReference} representation isn't and thats what we
     * compare when check for equality of the original and the shuffled builder
     */
    @Override
    protected String[] shuffleProtectedFields() {
        return SHUFFLE_PROTECTED_FIELDS;
    }

    @Override
    protected GetResponse executeGet(GetRequest getRequest) {
        assertThat(getRequest.index(), Matchers.equalTo(indexedDocumentIndex));
        assertThat(getRequest.id(), Matchers.equalTo(indexedDocumentId));
        assertThat(getRequest.routing(), Matchers.equalTo(indexedDocumentRouting));
        assertThat(getRequest.preference(), Matchers.equalTo(indexedDocumentPreference));
        assertThat(getRequest.version(), Matchers.equalTo(indexedDocumentVersion));
        if (indexedDocumentExists) {
            return new GetResponse(
                new GetResult(
                    indexedDocumentIndex,
                    indexedDocumentId,
                    0,
                    1,
                    0L,
                    true,
                    documentSource.iterator().next(),
                    Collections.emptyMap(),
                    Collections.emptyMap()
                )
            );
        } else {
            return new GetResponse(
                new GetResult(
                    indexedDocumentIndex,
                    indexedDocumentId,
                    UNASSIGNED_SEQ_NO,
                    0,
                    -1,
                    false,
                    null,
                    Collections.emptyMap(),
                    Collections.emptyMap()
                )
            );
        }
    }

    @Override
    protected void doAssertLuceneQuery(PercolateQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, Matchers.instanceOf(PercolateQuery.class));
        PercolateQuery percolateQuery = (PercolateQuery) query;
        assertThat(percolateQuery.getDocuments(), Matchers.equalTo(documentSource));
    }

    @Override
    public void testMustRewrite() throws IOException {
        PercolateQueryBuilder pqb = doCreateTestQueryBuilder(true);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> pqb.toQuery(createShardContext()));
        assertThat(e.getMessage(), equalTo("query builder must be rewritten first"));
        QueryBuilder rewrite = rewriteAndFetch(pqb, createShardContext());
        PercolateQueryBuilder geoShapeQueryBuilder = new PercolateQueryBuilder(pqb.getField(), documentSource, MediaTypeRegistry.JSON);
        assertEquals(geoShapeQueryBuilder, rewrite);
    }

    public void testIndexedDocumentDoesNotExist() throws IOException {
        indexedDocumentExists = false;
        PercolateQueryBuilder pqb = doCreateTestQueryBuilder(true);
        ResourceNotFoundException e = expectThrows(ResourceNotFoundException.class, () -> rewriteAndFetch(pqb, createShardContext()));
        String expectedString = "indexed document [" + indexedDocumentIndex + "/" + indexedDocumentId + "] couldn't be found";
        assertThat(e.getMessage(), equalTo(expectedString));
    }

    @Override
    protected Set<String> getObjectsHoldingArbitraryContent() {
        // document contains arbitrary content, no error expected when an object is added to it
        return new HashSet<>(
            Arrays.asList(PercolateQueryBuilder.DOCUMENT_FIELD.getPreferredName(), PercolateQueryBuilder.DOCUMENTS_FIELD.getPreferredName())
        );
    }

    public void testRequiredParameters() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            new PercolateQueryBuilder(null, new BytesArray("{}"), MediaTypeRegistry.JSON);
        });
        assertThat(e.getMessage(), equalTo("[field] is a required argument"));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> new PercolateQueryBuilder("_field", (List<BytesReference>) null, MediaTypeRegistry.JSON)
        );
        assertThat(e.getMessage(), equalTo("[document] is a required argument"));

        e = expectThrows(IllegalArgumentException.class, () -> { new PercolateQueryBuilder(null, "_index", "_id", null, null, null); });
        assertThat(e.getMessage(), equalTo("[field] is a required argument"));

        e = expectThrows(IllegalArgumentException.class, () -> { new PercolateQueryBuilder("_field", null, "_id", null, null, null); });
        assertThat(e.getMessage(), equalTo("[index] is a required argument"));

        e = expectThrows(IllegalArgumentException.class, () -> { new PercolateQueryBuilder("_field", "_index", null, null, null, null); });
        assertThat(e.getMessage(), equalTo("[id] is a required argument"));
    }

    public void testFromJsonNoDocumentType() throws IOException {
        QueryShardContext queryShardContext = createShardContext();
        QueryBuilder queryBuilder = parseQuery("{\"percolate\" : { \"document\": {}, \"field\":\"" + queryField + "\"}}");
        queryBuilder.toQuery(queryShardContext);
    }

    public void testFromJsonNoType() throws IOException {
        indexedDocumentIndex = randomAlphaOfLength(4);
        indexedDocumentId = randomAlphaOfLength(4);
        indexedDocumentVersion = Versions.MATCH_ANY;
        documentSource = Collections.singletonList(randomSource(new HashSet<>()));

        QueryShardContext queryShardContext = createShardContext();
        QueryBuilder queryBuilder = parseQuery(
            "{\"percolate\" : { \"index\": \""
                + indexedDocumentIndex
                + "\", \"id\": \""
                + indexedDocumentId
                + "\", \"field\":\""
                + queryField
                + "\"}}"
        );
        rewriteAndFetch(queryBuilder, queryShardContext).toQuery(queryShardContext);
    }

    public void testBothDocumentAndDocumentsSpecified() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> parseQuery("{\"percolate\" : { \"document\": {}, \"documents\": [{}, {}], \"field\":\"" + queryField + "\"}}")
        );
        assertThat(e.getMessage(), containsString("The following fields are not allowed together: [document, documents]"));
    }

    private static BytesReference randomSource(Set<String> usedFields) {
        try {
            // If we create two source that have the same field, but these fields have different kind of values (str vs. lng) then
            // when these source get indexed, indexing can fail. To solve this test issue, we should generate source that
            // always have unique fields:
            Map<String, ?> source;
            boolean duplicateField;
            do {
                duplicateField = false;
                source = RandomDocumentPicks.randomSource(random());
                for (String field : source.keySet()) {
                    if (usedFields.add(field) == false) {
                        duplicateField = true;
                        break;
                    }
                }
            } while (duplicateField);

            XContentBuilder xContent = XContentFactory.jsonBuilder();
            xContent.map(source);
            return BytesReference.bytes(xContent);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Test that this query is never cacheable
     */
    @Override
    public void testCacheability() throws IOException {
        PercolateQueryBuilder queryBuilder = createTestQueryBuilder();
        QueryShardContext context = createShardContext();
        assert context.isCacheable();
        QueryBuilder rewritten = rewriteQuery(queryBuilder, new QueryShardContext(context));
        assertNotNull(rewritten.toQuery(context));
        assertFalse("query should not be cacheable: " + queryBuilder.toString(), context.isCacheable());
    }

    @Override
    protected boolean builderGeneratesCacheableQueries() {
        return false;
    }

    public void testSerializationFailsUnlessFetched() throws IOException {
        QueryBuilder builder = doCreateTestQueryBuilder(true);
        QueryBuilder queryBuilder = Rewriteable.rewrite(builder, createShardContext());
        IllegalStateException ise = expectThrows(IllegalStateException.class, () -> queryBuilder.writeTo(new BytesStreamOutput(10)));
        assertEquals(ise.getMessage(), "supplier must be null, can't serialize suppliers, missing a rewriteAndFetch?");
        builder = rewriteAndFetch(builder, createShardContext());
        builder.writeTo(new BytesStreamOutput(10));
    }

    public void testFieldAlias() throws IOException {
        QueryShardContext shardContext = createShardContext();

        PercolateQueryBuilder builder = doCreateTestQueryBuilder(false);
        QueryBuilder rewrittenBuilder = rewriteAndFetch(builder, shardContext);
        PercolateQuery query = (PercolateQuery) rewrittenBuilder.toQuery(shardContext);

        PercolateQueryBuilder aliasBuilder = new PercolateQueryBuilder(aliasField, builder.getDocuments(), builder.getXContentType());
        QueryBuilder rewrittenAliasBuilder = rewriteAndFetch(aliasBuilder, shardContext);
        PercolateQuery aliasQuery = (PercolateQuery) rewrittenAliasBuilder.toQuery(shardContext);

        assertEquals(query.getCandidateMatchesQuery(), aliasQuery.getCandidateMatchesQuery());
        assertEquals(query.getVerifiedMatchesQuery(), aliasQuery.getVerifiedMatchesQuery());
    }

    public void testSettingNameWhileRewriting() {
        String testName = "name1";
        QueryShardContext shardContext = createShardContext();
        PercolateQueryBuilder percolateQueryBuilder = doCreateTestQueryBuilder(true);
        percolateQueryBuilder.setName(testName);

        QueryBuilder rewrittenQueryBuilder = percolateQueryBuilder.doRewrite(shardContext);

        assertEquals(testName, ((PercolateQueryBuilder) rewrittenQueryBuilder).getQueryName());
        assertNotEquals(rewrittenQueryBuilder, percolateQueryBuilder);
    }

    public void testSettingNameWhileRewritingWhenDocumentSupplierAndSourceNotNull() {
        Supplier<BytesReference> supplier = () -> new BytesArray("{\"test\": \"test\"}");
        String testName = "name1";
        QueryShardContext shardContext = createShardContext();
        PercolateQueryBuilder percolateQueryBuilder = new PercolateQueryBuilder(queryField, supplier);
        percolateQueryBuilder.setName(testName);

        QueryBuilder rewrittenQueryBuilder = percolateQueryBuilder.doRewrite(shardContext);

        assertEquals(testName, ((PercolateQueryBuilder) rewrittenQueryBuilder).getQueryName());
        assertNotEquals(rewrittenQueryBuilder, percolateQueryBuilder);
    }

    public void testDisallowExpensiveQueries() {
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        when(queryShardContext.allowExpensiveQueries()).thenReturn(false);

        PercolateQueryBuilder queryBuilder = doCreateTestQueryBuilder(true);
        OpenSearchException e = expectThrows(OpenSearchException.class, () -> queryBuilder.toQuery(queryShardContext));
        assertEquals("[percolate] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", e.getMessage());
    }
}
