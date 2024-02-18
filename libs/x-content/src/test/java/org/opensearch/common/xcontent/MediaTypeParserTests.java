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

package org.opensearch.common.xcontent;

import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class MediaTypeParserTests extends OpenSearchTestCase {

    public void testJsonWithParameters() throws Exception {
        String mediaType = "application/json";
        assertThat(MediaTypeRegistry.parseMediaType(mediaType).getParameters(), equalTo(Collections.emptyMap()));
        assertThat(MediaTypeRegistry.parseMediaType(mediaType + ";").getParameters(), equalTo(Collections.emptyMap()));
        assertThat(MediaTypeRegistry.parseMediaType(mediaType + "; charset=UTF-8").getParameters(), equalTo(Map.of("charset", "utf-8")));
        assertThat(
            MediaTypeRegistry.parseMediaType(mediaType + "; custom=123;charset=UTF-8").getParameters(),
            equalTo(Map.of("charset", "utf-8", "custom", "123"))
        );
    }

    public void testWhiteSpaceInTypeSubtype() {
        String mediaType = " application/json ";
        assertThat(MediaTypeRegistry.parseMediaType(mediaType).getMediaType(), equalTo(XContentType.JSON));

        assertThat(
            MediaTypeRegistry.parseMediaType(mediaType + "; custom=123; charset=UTF-8").getParameters(),
            equalTo(Map.of("charset", "utf-8", "custom", "123"))
        );
        assertThat(
            MediaTypeRegistry.parseMediaType(mediaType + "; custom=123;\n charset=UTF-8").getParameters(),
            equalTo(Map.of("charset", "utf-8", "custom", "123"))
        );

        mediaType = " application / json ";
        assertThat(MediaTypeRegistry.parseMediaType(mediaType), is(nullValue()));
    }

    public void testInvalidParameters() {
        String mediaType = "application/json";
        assertThat(MediaTypeRegistry.parseMediaType(mediaType + "; keyvalueNoEqualsSign"), is(nullValue()));

        assertThat(MediaTypeRegistry.parseMediaType(mediaType + "; key = value"), is(nullValue()));
        assertThat(MediaTypeRegistry.parseMediaType(mediaType + "; key="), is(nullValue()));
    }
}
