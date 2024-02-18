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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.test;

import com.carrotsearch.randomizedtesting.RandomizedContext;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class AbstractXContentTestCaseTests extends OpenSearchTestCase {

    public void testInsertRandomFieldsAndShuffle() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("field", 1);
        }
        builder.endObject();
        BytesReference insertRandomFieldsAndShuffle = RandomizedContext.current()
            .runWithPrivateRandomness(
                1,
                () -> AbstractXContentTestCase.insertRandomFieldsAndShuffle(
                    BytesReference.bytes(builder),
                    MediaTypeRegistry.JSON,
                    true,
                    new String[] {},
                    null,
                    this::createParser
                )
            );
        try (XContentParser parser = createParser(MediaTypeRegistry.JSON.xContent(), insertRandomFieldsAndShuffle)) {
            Map<String, Object> mapOrdered = parser.mapOrdered();
            assertThat(mapOrdered.size(), equalTo(2));
            assertThat(mapOrdered.keySet().iterator().next(), not(equalTo("field")));
        }
    }
}
