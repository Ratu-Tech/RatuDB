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

package org.opensearch.common.xcontent.json;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;

import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentGenerator;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.util.Set;

/**
 * A JSON based content implementation using Jackson.
 */
public class JsonXContent implements XContent {
    public static final int DEFAULT_MAX_STRING_LEN = Integer.parseInt(
        System.getProperty("opensearch.xcontent.string.length.max", "50000000" /* ~50 Mb */)
    );

    public static XContentBuilder contentBuilder() throws IOException {
        return XContentBuilder.builder(jsonXContent);
    }

    private static final JsonFactory jsonFactory;

    public static final JsonXContent jsonXContent;

    static {
        jsonFactory = new JsonFactory();
        jsonFactory.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, true);
        jsonFactory.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        jsonFactory.configure(JsonFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW, false); // this trips on many mappings now...
        // Do not automatically close unclosed objects/arrays in com.fasterxml.jackson.core.json.UTF8JsonGenerator#close() method
        jsonFactory.configure(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT, false);
        jsonFactory.configure(JsonParser.Feature.STRICT_DUPLICATE_DETECTION, true);
        jsonFactory.setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(DEFAULT_MAX_STRING_LEN).build());
        jsonFactory.configure(StreamReadFeature.USE_FAST_DOUBLE_PARSER.mappedFeature(), true);
        jsonXContent = new JsonXContent();
    }

    private JsonXContent() {}

    @Override
    public MediaType mediaType() {
        return XContentType.JSON;
    }

    @Override
    public byte streamSeparator() {
        return '\n';
    }

    @Override
    public XContentGenerator createGenerator(OutputStream os, Set<String> includes, Set<String> excludes) throws IOException {
        return new JsonXContentGenerator(jsonFactory.createGenerator(os, JsonEncoding.UTF8), os, includes, excludes);
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry, DeprecationHandler deprecationHandler, String content)
        throws IOException {
        return new JsonXContentParser(xContentRegistry, deprecationHandler, jsonFactory.createParser(content));
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry, DeprecationHandler deprecationHandler, InputStream is)
        throws IOException {
        return new JsonXContentParser(xContentRegistry, deprecationHandler, jsonFactory.createParser(is));
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry, DeprecationHandler deprecationHandler, byte[] data)
        throws IOException {
        return new JsonXContentParser(xContentRegistry, deprecationHandler, jsonFactory.createParser(data));
    }

    @Override
    public XContentParser createParser(
        NamedXContentRegistry xContentRegistry,
        DeprecationHandler deprecationHandler,
        byte[] data,
        int offset,
        int length
    ) throws IOException {
        return new JsonXContentParser(xContentRegistry, deprecationHandler, jsonFactory.createParser(data, offset, length));
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry, DeprecationHandler deprecationHandler, Reader reader)
        throws IOException {
        return new JsonXContentParser(xContentRegistry, deprecationHandler, jsonFactory.createParser(reader));
    }
}
