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

package org.opensearch.action.get;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * A single multi get response.
 *
 * @opensearch.internal
 */
public class MultiGetItemResponse implements Writeable {

    private final GetResponse response;
    private final MultiGetResponse.Failure failure;

    public MultiGetItemResponse(GetResponse response, MultiGetResponse.Failure failure) {
        this.response = response;
        this.failure = failure;
    }

    MultiGetItemResponse(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            failure = new MultiGetResponse.Failure(in);
            response = null;
        } else {
            response = new GetResponse(in);
            failure = null;
        }
    }

    /**
     * The index name of the document.
     */
    public String getIndex() {
        if (failure != null) {
            return failure.getIndex();
        }
        return response.getIndex();
    }

    /**
     * The id of the document.
     */
    public String getId() {
        if (failure != null) {
            return failure.getId();
        }
        return response.getId();
    }

    /**
     * Is this a failed execution?
     */
    public boolean isFailed() {
        return failure != null;
    }

    /**
     * The actual get response, {@code null} if its a failure.
     */
    public GetResponse getResponse() {
        return this.response;
    }

    /**
     * The failure if relevant.
     */
    public MultiGetResponse.Failure getFailure() {
        return this.failure;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (failure != null) {
            out.writeBoolean(true);
            failure.writeTo(out);
        } else {
            out.writeBoolean(false);
            response.writeTo(out);
        }
    }
}
