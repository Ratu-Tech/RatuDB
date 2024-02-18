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

package org.opensearch.indices.mapping;

import org.opensearch.action.index.IndexResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.emptyIterable;

public class ConcurrentDynamicTemplateIT extends OpenSearchIntegTestCase {
    // see #3544
    public void testConcurrentDynamicMapping() throws Exception {
        final String fieldName = "field";
        final String mapping = "{ "
            + "\"dynamic_templates\": ["
            + "{ \""
            + fieldName
            + "\": {"
            + "\"path_match\": \"*\","
            + "\"mapping\": {"
            + "\"type\": \"text\","
            + "\"store\": true,"
            + "\"analyzer\": \"whitespace\" } } } ] }";
        // The 'fieldNames' array is used to help with retrieval of index terms
        // after testing

        int iters = scaledRandomIntBetween(5, 15);
        for (int i = 0; i < iters; i++) {
            cluster().wipeIndices("test");
            assertAcked(prepareCreate("test").setMapping(mapping));
            int numDocs = scaledRandomIntBetween(10, 100);
            final CountDownLatch latch = new CountDownLatch(numDocs);
            final List<Throwable> throwable = new CopyOnWriteArrayList<>();
            int currentID = 0;
            for (int j = 0; j < numDocs; j++) {
                Map<String, Object> source = new HashMap<>();
                source.put(fieldName, "test-user");
                client().prepareIndex("test")
                    .setId(Integer.toString(currentID++))
                    .setSource(source)
                    .execute(new ActionListener<IndexResponse>() {
                        @Override
                        public void onResponse(IndexResponse response) {
                            latch.countDown();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            throwable.add(e);
                            latch.countDown();
                        }
                    });
            }
            latch.await();
            assertThat(throwable, emptyIterable());
            refresh();
            assertHitCount(client().prepareSearch("test").setQuery(QueryBuilders.matchQuery(fieldName, "test-user")).get(), numDocs);
            assertHitCount(client().prepareSearch("test").setQuery(QueryBuilders.matchQuery(fieldName, "test user")).get(), 0);

        }
    }
}
