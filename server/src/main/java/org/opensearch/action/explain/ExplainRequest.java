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

package org.opensearch.action.explain;

import org.opensearch.Version;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ValidateActions;
import org.opensearch.action.support.single.shard.SingleShardRequest;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.internal.AliasFilter;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Explain request encapsulating the explain query and document identifier to get an explanation for.
 *
 * @opensearch.internal
 */
public class ExplainRequest extends SingleShardRequest<ExplainRequest> implements ToXContentObject {

    private static final ParseField QUERY_FIELD = new ParseField("query");

    private String id;
    private String routing;
    private String preference;
    private QueryBuilder query;
    private String[] storedFields;
    private FetchSourceContext fetchSourceContext;

    private AliasFilter filteringAlias = new AliasFilter(null, Strings.EMPTY_ARRAY);

    long nowInMillis;

    public ExplainRequest() {}

    public ExplainRequest(String index, String id) {
        this.index = index;
        this.id = id;
    }

    ExplainRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().before(Version.V_2_0_0)) {
            in.readString();
        }
        id = in.readString();
        routing = in.readOptionalString();
        preference = in.readOptionalString();
        query = in.readNamedWriteable(QueryBuilder.class);
        filteringAlias = new AliasFilter(in);
        storedFields = in.readOptionalStringArray();
        fetchSourceContext = in.readOptionalWriteable(FetchSourceContext::new);
        nowInMillis = in.readVLong();
    }

    public String id() {
        return id;
    }

    public ExplainRequest id(String id) {
        this.id = id;
        return this;
    }

    public String routing() {
        return routing;
    }

    public ExplainRequest routing(String routing) {
        this.routing = routing;
        return this;
    }

    /**
     * Simple sets the routing. Since the parent is only used to get to the right shard.
     */
    public ExplainRequest parent(String parent) {
        this.routing = parent;
        return this;
    }

    public String preference() {
        return preference;
    }

    public ExplainRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    public QueryBuilder query() {
        return query;
    }

    public ExplainRequest query(QueryBuilder query) {
        this.query = query;
        return this;
    }

    /**
     * Allows setting the {@link FetchSourceContext} for this request, controlling if and how _source should be returned.
     */
    public ExplainRequest fetchSourceContext(FetchSourceContext context) {
        this.fetchSourceContext = context;
        return this;
    }

    public FetchSourceContext fetchSourceContext() {
        return fetchSourceContext;
    }

    public String[] storedFields() {
        return storedFields;
    }

    public ExplainRequest storedFields(String[] fields) {
        this.storedFields = fields;
        return this;
    }

    public AliasFilter filteringAlias() {
        return filteringAlias;
    }

    public ExplainRequest filteringAlias(AliasFilter filteringAlias) {
        if (filteringAlias != null) {
            this.filteringAlias = filteringAlias;
        }

        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validateNonNullIndex();
        if (Strings.isEmpty(id)) {
            validationException = addValidationError("id is missing", validationException);
        }
        if (query == null) {
            validationException = ValidateActions.addValidationError("query is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().before(Version.V_2_0_0)) {
            out.writeString(MapperService.SINGLE_MAPPING_NAME);
        }
        out.writeString(id);
        out.writeOptionalString(routing);
        out.writeOptionalString(preference);
        out.writeNamedWriteable(query);
        filteringAlias.writeTo(out);
        out.writeOptionalStringArray(storedFields);
        out.writeOptionalWriteable(fetchSourceContext);
        out.writeVLong(nowInMillis);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(QUERY_FIELD.getPreferredName(), query);
        builder.endObject();
        return builder;
    }
}
