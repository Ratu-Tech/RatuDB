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

package org.opensearch.index;

import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.hamcrest.OpenSearchAssertions;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsString;

public class IndexRequestBuilderIT extends OpenSearchIntegTestCase {
    public void testSetSource() throws InterruptedException, ExecutionException {
        createIndex("test");
        Map<String, Object> map = new HashMap<>();
        map.put("test_field", "foobar");
        IndexRequestBuilder[] builders = new IndexRequestBuilder[] {
            client().prepareIndex("test").setSource("test_field", "foobar"),
            client().prepareIndex("test").setSource("{\"test_field\" : \"foobar\"}", MediaTypeRegistry.JSON),
            client().prepareIndex("test").setSource(new BytesArray("{\"test_field\" : \"foobar\"}"), MediaTypeRegistry.JSON),
            client().prepareIndex("test").setSource(new BytesArray("{\"test_field\" : \"foobar\"}"), MediaTypeRegistry.JSON),
            client().prepareIndex("test")
                .setSource(BytesReference.toBytes(new BytesArray("{\"test_field\" : \"foobar\"}")), MediaTypeRegistry.JSON),
            client().prepareIndex("test").setSource(map) };
        indexRandom(true, builders);
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.termQuery("test_field", "foobar")).get();
        OpenSearchAssertions.assertHitCount(searchResponse, builders.length);
    }

    public void testOddNumberOfSourceObjects() {
        try {
            client().prepareIndex("test").setSource("test_field", "foobar", new Object());
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("The number of object passed must be even but was [3]"));
        }
    }
}
