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

package org.opensearch.script;

import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.Writeable.Reader;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class StoredScriptSourceTests extends AbstractSerializingTestCase<StoredScriptSource> {

    @Override
    protected StoredScriptSource createTestInstance() {
        MediaType mediaType = randomFrom(MediaTypeRegistry.JSON, XContentType.YAML);
        try {
            XContentBuilder template = XContentBuilder.builder(mediaType.xContent());
            template.startObject();
            template.startObject("script");
            {
                template.field("lang", "mustache");
                template.startObject("source");
                template.startObject("query").startObject("match").field("title", "{{query_string}}").endObject();
                template.endObject();
                template.endObject();
            }
            template.endObject();
            template.endObject();
            Map<String, String> options = new HashMap<>();
            if (randomBoolean()) {
                options.put(Script.CONTENT_TYPE_OPTION, mediaType.mediaType());
            }
            return StoredScriptSource.parse(BytesReference.bytes(template), mediaType);
        } catch (IOException e) {
            throw new AssertionError("Failed to create test instance", e);
        }
    }

    @Override
    protected StoredScriptSource doParseInstance(XContentParser parser) {
        return StoredScriptSource.fromXContent(parser, false);
    }

    @Override
    protected Reader<StoredScriptSource> instanceReader() {
        return StoredScriptSource::new;
    }

    @Override
    protected StoredScriptSource mutateInstance(StoredScriptSource instance) throws IOException {
        String source = instance.getSource();
        String lang = instance.getLang();
        Map<String, String> options = instance.getOptions();

        MediaType newXContentType = randomFrom(MediaTypeRegistry.JSON, XContentType.YAML);
        XContentBuilder newTemplate = XContentBuilder.builder(newXContentType.xContent());
        newTemplate.startObject();
        newTemplate.startObject("query");
        newTemplate.startObject("match");
        newTemplate.field("body", "{{query_string}}");
        newTemplate.endObject();
        newTemplate.endObject();
        newTemplate.endObject();

        switch (between(0, 2)) {
            case 0:
                source = newTemplate.toString();
                break;
            case 1:
                lang = randomAlphaOfLengthBetween(1, 20);
                break;
            case 2:
            default:
                options = new HashMap<>(options);
                options.put(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        }
        return new StoredScriptSource(lang, source, options);
    }
}
