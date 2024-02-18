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

package org.opensearch.action.fieldcaps;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.ValidateActions;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Transport request for retrieving field capabilities for an explicit list of fields
 */
public final class FieldCapabilitiesRequest extends ActionRequest implements IndicesRequest.Replaceable, ToXContentObject {
    public static final String NAME = "field_caps_request";

    private String[] indices = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();
    private String[] fields = Strings.EMPTY_ARRAY;
    private boolean includeUnmapped = false;
    // pkg private API mainly for cross cluster search to signal that we do multiple reductions ie. the results should not be merged
    private boolean mergeResults = true;
    private QueryBuilder indexFilter;
    private Long nowInMillis;

    public FieldCapabilitiesRequest(StreamInput in) throws IOException {
        super(in);
        fields = in.readStringArray();
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        mergeResults = in.readBoolean();
        includeUnmapped = in.readBoolean();
        indexFilter = in.readOptionalNamedWriteable(QueryBuilder.class);
        nowInMillis = in.readOptionalLong();
    }

    public FieldCapabilitiesRequest() {}

    /**
     * Returns <code>true</code> iff the results should be merged.
     * <p>
     * Note that when using the high-level REST client, results are always merged (this flag is always considered 'true').
     */
    boolean isMergeResults() {
        return mergeResults;
    }

    /**
     * If set to <code>true</code> the response will contain only a merged view of the per index field capabilities.
     * Otherwise only unmerged per index field capabilities are returned.
     * <p>
     * Note that when using the high-level REST client, results are always merged (this flag is always considered 'true').
     */
    void setMergeResults(boolean mergeResults) {
        this.mergeResults = mergeResults;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(fields);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeBoolean(mergeResults);
        out.writeBoolean(includeUnmapped);
        out.writeOptionalNamedWriteable(indexFilter);
        out.writeOptionalLong(nowInMillis);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (indexFilter != null) {
            builder.field("index_filter", indexFilter);
        }
        builder.endObject();
        return builder;
    }

    /**
     * The list of field names to retrieve
     */
    public FieldCapabilitiesRequest fields(String... fields) {
        if (fields == null || fields.length == 0) {
            throw new IllegalArgumentException("specified fields can't be null or empty");
        }
        Set<String> fieldSet = new HashSet<>(Arrays.asList(fields));
        this.fields = fieldSet.toArray(new String[0]);
        return this;
    }

    public String[] fields() {
        return fields;
    }

    /**
     * The list of indices to lookup
     */
    public FieldCapabilitiesRequest indices(String... indices) {
        this.indices = Objects.requireNonNull(indices, "indices must not be null");
        return this;
    }

    public FieldCapabilitiesRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = Objects.requireNonNull(indicesOptions, "indices options must not be null");
        return this;
    }

    public FieldCapabilitiesRequest includeUnmapped(boolean includeUnmapped) {
        this.includeUnmapped = includeUnmapped;
        return this;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    public boolean includeUnmapped() {
        return includeUnmapped;
    }

    /**
     * Allows to filter indices if the provided {@link QueryBuilder} rewrites to `match_none` on every shard.
     */
    public FieldCapabilitiesRequest indexFilter(QueryBuilder indexFilter) {
        this.indexFilter = indexFilter;
        return this;
    }

    public QueryBuilder indexFilter() {
        return indexFilter;
    }

    Long nowInMillis() {
        return nowInMillis;
    }

    void nowInMillis(long nowInMillis) {
        this.nowInMillis = nowInMillis;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (fields == null || fields.length == 0) {
            validationException = ValidateActions.addValidationError("no fields specified", validationException);
        }
        return validationException;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldCapabilitiesRequest that = (FieldCapabilitiesRequest) o;
        return includeUnmapped == that.includeUnmapped
            && mergeResults == that.mergeResults
            && Arrays.equals(indices, that.indices)
            && indicesOptions.equals(that.indicesOptions)
            && Arrays.equals(fields, that.fields)
            && Objects.equals(indexFilter, that.indexFilter)
            && Objects.equals(nowInMillis, that.nowInMillis);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(indicesOptions, includeUnmapped, mergeResults, indexFilter, nowInMillis);
        result = 31 * result + Arrays.hashCode(indices);
        result = 31 * result + Arrays.hashCode(fields);
        return result;
    }
}
