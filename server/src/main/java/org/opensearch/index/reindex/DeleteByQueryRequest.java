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

package org.opensearch.index.reindex;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryBuilder;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Creates a new {@link DeleteByQueryRequest} that uses scrolling and bulk requests to delete all documents matching
 * the query. This can have performance as well as visibility implications.
 * <p>
 * Delete-by-query now has the following semantics:
 * <ul>
 *     <li>it's {@code non-atomic}, a delete-by-query may fail at any time while some documents matching the query have already been
 *     deleted</li>
 *     <li>it's {@code syntactic sugar}, a delete-by-query is equivalent to a scroll search and corresponding bulk-deletes by ID</li>
 *     <li>it's executed on a {@code point-in-time} snapshot, a delete-by-query will only delete the documents that are visible at the
 *     point in time the delete-by-query was started, equivalent to the scroll API</li>
 *     <li>it's {@code consistent}, a delete-by-query will yield consistent results across all replicas of a shard</li>
 *     <li>it's {@code forward-compatible}, a delete-by-query will only send IDs to the shards as deletes such that no queries are
 *     stored in the transaction logs that might not be supported in the future.</li>
 *     <li>it's results won't be visible until the index is refreshed.</li>
 * </ul>
 *
 * @opensearch.internal
 */
public class DeleteByQueryRequest extends AbstractBulkByScrollRequest<DeleteByQueryRequest>
    implements
        IndicesRequest.Replaceable,
        ToXContentObject {

    public DeleteByQueryRequest() {
        this(new SearchRequest());
    }

    public DeleteByQueryRequest(String... indices) {
        this(new SearchRequest(indices));
    }

    DeleteByQueryRequest(SearchRequest search) {
        this(search, true);
    }

    public DeleteByQueryRequest(StreamInput in) throws IOException {
        super(in);
    }

    private DeleteByQueryRequest(SearchRequest search, boolean setDefaults) {
        super(search, setDefaults);
        // Delete-By-Query does not require the source
        if (setDefaults) {
            search.source().fetchSource(false);
        }
    }

    /**
     * Set the query for selective delete
     */
    public DeleteByQueryRequest setQuery(QueryBuilder query) {
        if (query != null) {
            getSearchRequest().source().query(query);
        }
        return this;
    }

    /**
     * Set routing limiting the process to the shards that match that routing value
     */
    public DeleteByQueryRequest setRouting(String routing) {
        if (routing != null) {
            getSearchRequest().routing(routing);
        }
        return this;
    }

    /**
     * The scroll size to control number of documents processed per batch
     */
    public DeleteByQueryRequest setBatchSize(int size) {
        getSearchRequest().source().size(size);
        return this;
    }

    /**
     * Set the IndicesOptions for controlling unavailable indices
     */
    public DeleteByQueryRequest setIndicesOptions(IndicesOptions indicesOptions) {
        getSearchRequest().indicesOptions(indicesOptions);
        return this;
    }

    /**
     * Gets the batch size for this request
     */
    public int getBatchSize() {
        return getSearchRequest().source().size();
    }

    /**
     * Gets the routing value used for this request
     */
    public String getRouting() {
        return getSearchRequest().routing();
    }

    @Override
    protected DeleteByQueryRequest self() {
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException e = super.validate();
        if (getSearchRequest().indices() == null || getSearchRequest().indices().length == 0) {
            e = addValidationError("use _all if you really want to delete from all existing indexes", e);
        }
        if (getSearchRequest() == null || getSearchRequest().source() == null) {
            e = addValidationError("source is missing", e);
        } else if (getSearchRequest().source().query() == null) {
            e = addValidationError("query is missing", e);
        }
        return e;
    }

    @Override
    public DeleteByQueryRequest forSlice(TaskId slicingTask, SearchRequest slice, int totalSlices) {
        return doForSlice(new DeleteByQueryRequest(slice, false), slicingTask, totalSlices);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("delete-by-query ");
        searchToString(b);
        return b.toString();
    }

    // delete by query deletes all documents that match a query. The indices and indices options that affect how
    // indices are resolved depend entirely on the inner search request. That's why the following methods delegate to it.
    @Override
    public IndicesRequest indices(String... indices) {
        assert getSearchRequest() != null;
        getSearchRequest().indices(indices);
        return this;
    }

    @Override
    public String[] indices() {
        assert getSearchRequest() != null;
        return getSearchRequest().indices();
    }

    @Override
    public IndicesOptions indicesOptions() {
        assert getSearchRequest() != null;
        return getSearchRequest().indicesOptions();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        getSearchRequest().source().innerToXContent(builder, params);
        builder.endObject();
        return builder;
    }
}
