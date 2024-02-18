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

package org.opensearch.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

import static java.util.Collections.singletonMap;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * REST response in bytes
 *
 * @opensearch.api
 */
public class BytesRestResponse extends RestResponse {

    public static final String TEXT_CONTENT_TYPE = "text/plain; charset=UTF-8";

    private static final String STATUS = "status";

    private static final Logger SUPPRESSED_ERROR_LOGGER = LogManager.getLogger("rest.suppressed");

    private final RestStatus status;
    private final BytesReference content;
    private final String contentType;

    /**
     * Creates a new response based on {@link XContentBuilder}.
     */
    public BytesRestResponse(RestStatus status, XContentBuilder builder) {
        this(status, builder.contentType().mediaType(), BytesReference.bytes(builder));
    }

    /**
     * Creates a new plain text response.
     */
    public BytesRestResponse(RestStatus status, String content) {
        this(status, TEXT_CONTENT_TYPE, new BytesArray(content));
    }

    /**
     * Creates a new plain text response.
     */
    public BytesRestResponse(RestStatus status, String contentType, String content) {
        this(status, contentType, new BytesArray(content));
    }

    /**
     * Creates a binary response.
     */
    public BytesRestResponse(RestStatus status, String contentType, byte[] content) {
        this(status, contentType, new BytesArray(content));
    }

    /**
     * Creates a binary response.
     */
    public BytesRestResponse(RestStatus status, String contentType, BytesReference content) {
        this.status = status;
        this.content = content;
        this.contentType = contentType;
    }

    public BytesRestResponse(RestChannel channel, Exception e) throws IOException {
        this(channel, ExceptionsHelper.status(e), e);
    }

    public BytesRestResponse(RestChannel channel, RestStatus status, Exception e) throws IOException {
        ToXContent.Params params = paramsFromRequest(channel.request());
        if (params.paramAsBoolean(
            OpenSearchException.REST_EXCEPTION_SKIP_STACK_TRACE,
            OpenSearchException.REST_EXCEPTION_SKIP_STACK_TRACE_DEFAULT
        ) && e != null) {
            // log exception only if it is not returned in the response
            Supplier<?> messageSupplier = () -> new ParameterizedMessage(
                "path: {}, params: {}",
                channel.request().rawPath(),
                channel.request().params()
            );
            if (status.getStatus() < 500) {
                SUPPRESSED_ERROR_LOGGER.debug(messageSupplier, e);
            } else {
                SUPPRESSED_ERROR_LOGGER.warn(messageSupplier, e);
            }
        }
        this.status = status;
        try (XContentBuilder builder = channel.newErrorBuilder()) {
            build(builder, params, status, channel.detailedErrorsEnabled(), e);
            this.content = BytesReference.bytes(builder);
            this.contentType = builder.contentType().mediaType();
        }
        if (e instanceof OpenSearchException) {
            copyHeaders(((OpenSearchException) e));
        }
    }

    @Override
    public String contentType() {
        return this.contentType;
    }

    @Override
    public BytesReference content() {
        return this.content;
    }

    @Override
    public RestStatus status() {
        return this.status;
    }

    private ToXContent.Params paramsFromRequest(RestRequest restRequest) {
        ToXContent.Params params = restRequest;
        if (params.paramAsBoolean("error_trace", OpenSearchException.REST_EXCEPTION_SKIP_STACK_TRACE_DEFAULT == false)
            && false == skipStackTrace()) {
            params = new ToXContent.DelegatingMapParams(singletonMap(OpenSearchException.REST_EXCEPTION_SKIP_STACK_TRACE, "false"), params);
        }
        return params;
    }

    protected boolean skipStackTrace() {
        return false;
    }

    private void build(XContentBuilder builder, ToXContent.Params params, RestStatus status, boolean detailedErrorsEnabled, Exception e)
        throws IOException {
        builder.startObject();
        OpenSearchException.generateFailureXContent(builder, params, e, detailedErrorsEnabled);
        builder.field(STATUS, status.getStatus());
        builder.endObject();
    }

    static BytesRestResponse createSimpleErrorResponse(RestChannel channel, RestStatus status, String errorMessage) throws IOException {
        return new BytesRestResponse(
            status,
            channel.newErrorBuilder().startObject().field("error", errorMessage).field("status", status.getStatus()).endObject()
        );
    }

    public static OpenSearchStatusException errorFromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);

        OpenSearchException exception = null;
        RestStatus status = null;

        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            }
            if (STATUS.equals(currentFieldName)) {
                if (token != XContentParser.Token.FIELD_NAME) {
                    ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser);
                    status = RestStatus.fromCode(parser.intValue());
                }
            } else {
                exception = OpenSearchException.failureFromXContent(parser);
            }
        }

        if (exception == null) {
            throw new IllegalStateException("Failed to parse opensearch status exception: no exception was found");
        }

        OpenSearchStatusException result = new OpenSearchStatusException(exception.getMessage(), status, exception.getCause());
        for (String header : exception.getHeaderKeys()) {
            result.addHeader(header, exception.getHeader(header));
        }
        for (String metadata : exception.getMetadataKeys()) {
            result.addMetadata(metadata, exception.getMetadata(metadata));
        }
        return result;
    }
}
