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

package org.opensearch.action.termvectors;

import org.opensearch.OpenSearchParseException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.CompositeIndicesRequest;
import org.opensearch.action.RealtimeRequest;
import org.opensearch.action.ValidateActions;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * A single multi get request.
 *
 * @opensearch.internal
 */
public class MultiTermVectorsRequest extends ActionRequest
    implements
        Iterable<TermVectorsRequest>,
        CompositeIndicesRequest,
        RealtimeRequest {

    String preference;
    List<TermVectorsRequest> requests = new ArrayList<>();

    final Set<String> ids = new HashSet<>();

    public MultiTermVectorsRequest(StreamInput in) throws IOException {
        super(in);
        preference = in.readOptionalString();
        int size = in.readVInt();
        requests = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            requests.add(new TermVectorsRequest(in));
        }
    }

    public MultiTermVectorsRequest() {}

    public MultiTermVectorsRequest add(TermVectorsRequest termVectorsRequest) {
        requests.add(termVectorsRequest);
        return this;
    }

    public MultiTermVectorsRequest add(String index, String id) {
        requests.add(new TermVectorsRequest(index, id));
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (requests.isEmpty()) {
            validationException = ValidateActions.addValidationError("multi term vectors: no documents requested", validationException);
        } else {
            for (int i = 0; i < requests.size(); i++) {
                TermVectorsRequest termVectorsRequest = requests.get(i);
                ActionRequestValidationException validationExceptionForDoc = termVectorsRequest.validate();
                if (validationExceptionForDoc != null) {
                    validationException = ValidateActions.addValidationError(
                        "at multi term vectors for doc " + i,
                        validationExceptionForDoc
                    );
                }
            }
        }
        return validationException;
    }

    @Override
    public Iterator<TermVectorsRequest> iterator() {
        return Collections.unmodifiableCollection(requests).iterator();
    }

    public boolean isEmpty() {
        return requests.isEmpty() && ids.isEmpty();
    }

    public List<TermVectorsRequest> getRequests() {
        return requests;
    }

    public void add(TermVectorsRequest template, @Nullable XContentParser parser) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        if (parser != null) {
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if ("docs".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token != XContentParser.Token.START_OBJECT) {
                                throw new IllegalArgumentException("docs array element should include an object");
                            }
                            TermVectorsRequest termVectorsRequest = new TermVectorsRequest(template);
                            TermVectorsRequest.parseRequest(termVectorsRequest, parser);
                            add(termVectorsRequest);
                        }
                    } else if ("ids".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (!token.isValue()) {
                                throw new IllegalArgumentException("ids array element should only contain ids");
                            }
                            ids.add(parser.text());
                        }
                    } else {
                        throw new OpenSearchParseException("no parameter named [{}] and type ARRAY", currentFieldName);
                    }
                } else if (token == XContentParser.Token.START_OBJECT && currentFieldName != null) {
                    if ("parameters".equals(currentFieldName)) {
                        TermVectorsRequest.parseRequest(template, parser);
                    } else {
                        throw new OpenSearchParseException("no parameter named [{}] and type OBJECT", currentFieldName);
                    }
                } else if (currentFieldName != null) {
                    throw new OpenSearchParseException("_mtermvectors: Parameter [{}] not supported", currentFieldName);
                }
            }
        }
        for (String id : ids) {
            TermVectorsRequest curRequest = new TermVectorsRequest(template);
            curRequest.id(id);
            requests.add(curRequest);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(preference);
        out.writeCollection(requests);
    }

    public void ids(String[] ids) {
        for (String id : ids) {
            this.ids.add(id.replaceAll("\\s", ""));
        }
    }

    public int size() {
        return requests.size();
    }

    @Override
    public MultiTermVectorsRequest realtime(boolean realtime) {
        for (TermVectorsRequest request : requests) {
            request.realtime(realtime);
        }
        return this;
    }
}
