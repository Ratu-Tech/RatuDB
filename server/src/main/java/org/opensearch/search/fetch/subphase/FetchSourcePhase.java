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
import org.opensearch.OpenSearchException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.fetch.FetchSubPhaseProcessor;
import org.opensearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.Map;

/**
 * Fetches the document source during search phase
 *
 * @opensearch.internal
 */
public final class FetchSourcePhase implements FetchSubPhase {

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext fetchContext) {
        FetchSourceContext fetchSourceContext = fetchContext.fetchSourceContext();
        if (fetchSourceContext == null || fetchSourceContext.fetchSource() == false) {
            return null;
        }
        String index = fetchContext.getIndexName();
        assert fetchSourceContext.fetchSource();

        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(LeafReaderContext readerContext) {

            }

            @Override
            public void process(HitContext hitContext) {
                hitExecute(index, fetchSourceContext, hitContext);
            }
        };
    }

    private void hitExecute(String index, FetchSourceContext fetchSourceContext, HitContext hitContext) {

        final boolean nestedHit = hitContext.hit().getNestedIdentity() != null;
        SourceLookup source = hitContext.sourceLookup();

        // If source is disabled in the mapping, then attempt to return early.
        if (source.source() == null && source.internalSourceRef() == null) {
            if (containsFilters(fetchSourceContext)) {
                throw new IllegalArgumentException(
                    "unable to fetch fields from _source field: _source is disabled in the mappings " + "for index [" + index + "]"
                );
            }
            return;
        }

        // If this is a parent document and there are no source filters, then add the source as-is.
        if (nestedHit == false && containsFilters(fetchSourceContext) == false) {
            hitContext.hit().sourceRef(source.internalSourceRef());
            return;
        }

        // Otherwise, filter the source and add it to the hit.
        Object value = source.filter(fetchSourceContext);
        if (nestedHit) {
            value = getNestedSource((Map<String, Object>) value, hitContext);
        }

        try {
            final int initialCapacity = nestedHit ? 1024 : Math.min(1024, source.internalSourceRef().length());
            BytesStreamOutput streamOutput = new BytesStreamOutput(initialCapacity);
            XContentBuilder builder = new XContentBuilder(source.sourceContentType().xContent(), streamOutput);
            if (value != null) {
                builder.value(value);
            } else {
                // This happens if the source filtering could not find the specified in the _source.
                // Just doing `builder.value(null)` is valid, but the xcontent validation can't detect what format
                // it is. In certain cases, for example response serialization we fail if no xcontent type can't be
                // detected. So instead we just return an empty top level object. Also this is in inline with what was
                // being return in this situation in 5.x and earlier.
                builder.startObject();
                builder.endObject();
            }
            hitContext.hit().sourceRef(BytesReference.bytes(builder));
        } catch (IOException e) {
            throw new OpenSearchException("Error filtering source", e);
        }
    }

    private static boolean containsFilters(FetchSourceContext context) {
        return context.includes().length != 0 || context.excludes().length != 0;
    }

    private Map<String, Object> getNestedSource(Map<String, Object> sourceAsMap, HitContext hitContext) {
        for (SearchHit.NestedIdentity o = hitContext.hit().getNestedIdentity(); o != null; o = o.getChild()) {
            sourceAsMap = (Map<String, Object>) sourceAsMap.get(o.getField().string());
            if (sourceAsMap == null) {
                return null;
            }
        }
        return sourceAsMap;
    }
}
