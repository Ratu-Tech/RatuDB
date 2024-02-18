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

package org.opensearch.action.search;

import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.core.action.ActionListener;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.dfs.AggregatedDfs;
import org.opensearch.search.dfs.DfsSearchResult;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.transport.Transport;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

/**
 * Async action for DFS Query Then Fetch
 *
 * @opensearch.internal
 */
final class SearchDfsQueryThenFetchAsyncAction extends AbstractSearchAsyncAction<DfsSearchResult> {

    private final SearchPhaseController searchPhaseController;

    private final QueryPhaseResultConsumer queryPhaseResultConsumer;

    SearchDfsQueryThenFetchAsyncAction(
        final Logger logger,
        final SearchTransportService searchTransportService,
        final BiFunction<String, String, Transport.Connection> nodeIdToConnection,
        final Map<String, AliasFilter> aliasFilter,
        final Map<String, Float> concreteIndexBoosts,
        final Map<String, Set<String>> indexRoutings,
        final SearchPhaseController searchPhaseController,
        final Executor executor,
        final QueryPhaseResultConsumer queryPhaseResultConsumer,
        final SearchRequest request,
        final ActionListener<SearchResponse> listener,
        final GroupShardsIterator<SearchShardIterator> shardsIts,
        final TransportSearchAction.SearchTimeProvider timeProvider,
        final ClusterState clusterState,
        final SearchTask task,
        SearchResponse.Clusters clusters,
        SearchRequestOperationsListener searchRequestOperationsListener
    ) {
        super(
            SearchPhaseName.DFS_PRE_QUERY.getName(),
            logger,
            searchTransportService,
            nodeIdToConnection,
            aliasFilter,
            concreteIndexBoosts,
            indexRoutings,
            executor,
            request,
            listener,
            shardsIts,
            timeProvider,
            clusterState,
            task,
            new ArraySearchPhaseResults<>(shardsIts.size()),
            request.getMaxConcurrentShardRequests(),
            clusters,
            searchRequestOperationsListener
        );
        this.queryPhaseResultConsumer = queryPhaseResultConsumer;
        this.searchPhaseController = searchPhaseController;
        SearchProgressListener progressListener = task.getProgressListener();
        SearchSourceBuilder sourceBuilder = request.source();
        progressListener.notifyListShards(
            SearchProgressListener.buildSearchShards(this.shardsIts),
            SearchProgressListener.buildSearchShards(toSkipShardsIts),
            clusters,
            sourceBuilder == null || sourceBuilder.size() != 0
        );
    }

    @Override
    protected void executePhaseOnShard(
        final SearchShardIterator shardIt,
        final SearchShardTarget shard,
        final SearchActionListener<DfsSearchResult> listener
    ) {
        getSearchTransport().sendExecuteDfs(
            getConnection(shard.getClusterAlias(), shard.getNodeId()),
            buildShardSearchRequest(shardIt),
            getTask(),
            listener
        );
    }

    @Override
    protected SearchPhase getNextPhase(final SearchPhaseResults<DfsSearchResult> results, SearchPhaseContext context) {
        final List<DfsSearchResult> dfsSearchResults = results.getAtomicArray().asList();
        final AggregatedDfs aggregatedDfs = searchPhaseController.aggregateDfs(dfsSearchResults);

        return new DfsQueryPhase(
            dfsSearchResults,
            aggregatedDfs,
            queryPhaseResultConsumer,
            (queryResults) -> new FetchSearchPhase(queryResults, searchPhaseController, aggregatedDfs, context),
            context
        );
    }
}
