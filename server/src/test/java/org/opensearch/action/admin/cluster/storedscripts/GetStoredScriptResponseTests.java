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

package org.opensearch.action.admin.cluster.storedscripts;

import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.script.Script;
import org.opensearch.script.StoredScriptSource;
import org.opensearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.function.Predicate;

public class GetStoredScriptResponseTests extends AbstractSerializingTestCase<GetStoredScriptResponse> {

    @Override
    protected GetStoredScriptResponse doParseInstance(XContentParser parser) throws IOException {
        return GetStoredScriptResponse.fromXContent(parser);
    }

    @Override
    protected GetStoredScriptResponse createTestInstance() {
        return new GetStoredScriptResponse(randomAlphaOfLengthBetween(1, 10), randomScriptSource());
    }

    @Override
    protected Writeable.Reader<GetStoredScriptResponse> instanceReader() {
        return GetStoredScriptResponse::new;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return s -> "script.options".equals(s);
    }

    private static StoredScriptSource randomScriptSource() {
        final String lang = randomFrom("lang", "painless", "mustache");
        final String source = randomAlphaOfLengthBetween(1, 10);
        final Map<String, String> options = randomBoolean()
            ? Collections.singletonMap(Script.CONTENT_TYPE_OPTION, MediaTypeRegistry.JSON.mediaType())
            : Collections.emptyMap();
        return new StoredScriptSource(lang, source, options);
    }
}
