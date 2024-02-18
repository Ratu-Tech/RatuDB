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

package org.opensearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.memory.MemoryIndex;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchSubPhase.HitContext;
import org.opensearch.search.fetch.FetchSubPhaseProcessor;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FetchSourcePhaseTests extends OpenSearchTestCase {

    public void testFetchSource() throws IOException {
        XContentBuilder source = XContentFactory.jsonBuilder().startObject().field("field", "value").endObject();
        HitContext hitContext = hitExecute(source, true, null, null);
        assertEquals(Collections.singletonMap("field", "value"), hitContext.hit().getSourceAsMap());
    }

    public void testBasicFiltering() throws IOException {
        XContentBuilder source = XContentFactory.jsonBuilder().startObject().field("field1", "value").field("field2", "value2").endObject();
        HitContext hitContext = hitExecute(source, false, null, null);
        assertNull(hitContext.hit().getSourceAsMap());

        hitContext = hitExecute(source, true, "field1", null);
        assertEquals(Collections.singletonMap("field1", "value"), hitContext.hit().getSourceAsMap());

        hitContext = hitExecute(source, true, "hello", null);
        assertEquals(Collections.emptyMap(), hitContext.hit().getSourceAsMap());

        hitContext = hitExecute(source, true, "*", "field2");
        assertEquals(Collections.singletonMap("field1", "value"), hitContext.hit().getSourceAsMap());
    }

    public void testMultipleFiltering() throws IOException {
        XContentBuilder source = XContentFactory.jsonBuilder().startObject().field("field", "value").field("field2", "value2").endObject();
        HitContext hitContext = hitExecuteMultiple(source, true, new String[] { "*.notexisting", "field" }, null);
        assertEquals(Collections.singletonMap("field", "value"), hitContext.hit().getSourceAsMap());

        hitContext = hitExecuteMultiple(source, true, new String[] { "field.notexisting.*", "field" }, null);
        assertEquals(Collections.singletonMap("field", "value"), hitContext.hit().getSourceAsMap());
    }

    public void testNestedSource() throws IOException {
        Map<String, Object> expectedNested = Collections.singletonMap("nested2", Collections.singletonMap("field", "value0"));
        XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject()
            .field("field", "value")
            .field("field2", "value2")
            .field("nested1", expectedNested)
            .endObject();
        HitContext hitContext = hitExecuteMultiple(source, true, null, null, new SearchHit.NestedIdentity("nested1", 0, null));
        assertEquals(expectedNested, hitContext.hit().getSourceAsMap());
        hitContext = hitExecuteMultiple(source, true, new String[] { "invalid" }, null, new SearchHit.NestedIdentity("nested1", 0, null));
        assertEquals(Collections.emptyMap(), hitContext.hit().getSourceAsMap());

        hitContext = hitExecuteMultiple(
            source,
            true,
            null,
            null,
            new SearchHit.NestedIdentity("nested1", 0, new SearchHit.NestedIdentity("nested2", 0, null))
        );
        assertEquals(Collections.singletonMap("field", "value0"), hitContext.hit().getSourceAsMap());

        hitContext = hitExecuteMultiple(
            source,
            true,
            new String[] { "invalid" },
            null,
            new SearchHit.NestedIdentity("nested1", 0, new SearchHit.NestedIdentity("nested2", 0, null))
        );
        assertEquals(Collections.emptyMap(), hitContext.hit().getSourceAsMap());
    }

    public void testSourceDisabled() throws IOException {
        HitContext hitContext = hitExecute(null, true, null, null);
        assertNull(hitContext.hit().getSourceAsMap());

        hitContext = hitExecute(null, false, null, null);
        assertNull(hitContext.hit().getSourceAsMap());

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> hitExecute(null, true, "field1", null));
        assertEquals(
            "unable to fetch fields from _source field: _source is disabled in the mappings " + "for index [index]",
            exception.getMessage()
        );

        exception = expectThrows(
            IllegalArgumentException.class,
            () -> hitExecuteMultiple(null, true, new String[] { "*" }, new String[] { "field2" })
        );
        assertEquals(
            "unable to fetch fields from _source field: _source is disabled in the mappings " + "for index [index]",
            exception.getMessage()
        );
    }

    public void testNestedSourceWithSourceDisabled() throws IOException {
        HitContext hitContext = hitExecute(null, true, null, null, new SearchHit.NestedIdentity("nested1", 0, null));
        assertNull(hitContext.hit().getSourceAsMap());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> hitExecute(null, true, "field1", null, new SearchHit.NestedIdentity("nested1", 0, null))
        );
        assertEquals(
            "unable to fetch fields from _source field: _source is disabled in the mappings " + "for index [index]",
            e.getMessage()
        );
    }

    private HitContext hitExecute(XContentBuilder source, boolean fetchSource, String include, String exclude) throws IOException {
        return hitExecute(source, fetchSource, include, exclude, null);
    }

    private HitContext hitExecute(
        XContentBuilder source,
        boolean fetchSource,
        String include,
        String exclude,
        SearchHit.NestedIdentity nestedIdentity
    ) throws IOException {
        return hitExecuteMultiple(
            source,
            fetchSource,
            include == null ? Strings.EMPTY_ARRAY : new String[] { include },
            exclude == null ? Strings.EMPTY_ARRAY : new String[] { exclude },
            nestedIdentity
        );
    }

    private HitContext hitExecuteMultiple(XContentBuilder source, boolean fetchSource, String[] includes, String[] excludes)
        throws IOException {
        return hitExecuteMultiple(source, fetchSource, includes, excludes, null);
    }

    private HitContext hitExecuteMultiple(
        XContentBuilder source,
        boolean fetchSource,
        String[] includes,
        String[] excludes,
        SearchHit.NestedIdentity nestedIdentity
    ) throws IOException {
        FetchSourceContext fetchSourceContext = new FetchSourceContext(fetchSource, includes, excludes);
        FetchContext fetchContext = mock(FetchContext.class);
        when(fetchContext.fetchSourceContext()).thenReturn(fetchSourceContext);
        when(fetchContext.getIndexName()).thenReturn("index");

        final SearchHit searchHit = new SearchHit(1, null, nestedIdentity, null, null);

        // We don't need a real index, just a LeafReaderContext which cannot be mocked.
        MemoryIndex index = new MemoryIndex();
        LeafReaderContext leafReaderContext = index.createSearcher().getIndexReader().leaves().get(0);
        HitContext hitContext = new HitContext(searchHit, leafReaderContext, 1, new SourceLookup());
        hitContext.sourceLookup().setSource(source == null ? null : BytesReference.bytes(source));

        FetchSourcePhase phase = new FetchSourcePhase();
        FetchSubPhaseProcessor processor = phase.getProcessor(fetchContext);
        if (fetchSource == false) {
            assertNull(processor);
        } else {
            assertNotNull(processor);
            processor.process(hitContext);
        }
        return hitContext;
    }

}
