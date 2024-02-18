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

package org.opensearch.search.aggregations.bucket.filter;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AggregationInitializationException;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregator.KeyedFilter;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Aggregation Factory for filters agg
 *
 * @opensearch.internal
 */
public class FiltersAggregatorFactory extends AggregatorFactory {

    private final String[] keys;
    private final Query[] filters;
    private volatile Weight[] weights;
    private final boolean keyed;
    private final boolean otherBucket;
    private final String otherBucketKey;

    public FiltersAggregatorFactory(
        String name,
        List<KeyedFilter> filters,
        boolean keyed,
        boolean otherBucket,
        String otherBucketKey,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactories,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, queryShardContext, parent, subFactories, metadata);
        this.keyed = keyed;
        this.otherBucket = otherBucket;
        this.otherBucketKey = otherBucketKey;
        keys = new String[filters.size()];
        this.filters = new Query[filters.size()];
        for (int i = 0; i < filters.size(); ++i) {
            KeyedFilter keyedFilter = filters.get(i);
            this.keys[i] = keyedFilter.key();
            this.filters[i] = keyedFilter.filter().toQuery(queryShardContext);
        }
    }

    /**
     * Returns the {@link Weight}s for this filter aggregation, creating it if
     * necessary. This is done lazily so that the {@link Weight}s are only
     * created if the aggregation collects documents reducing the overhead of
     * the aggregation in the case where no documents are collected.
     * <p>
     * Note: With concurrent segment search use case, multiple aggregation collectors executing
     * on different threads will try to fetch the weights. To handle the race condition there is
     * a synchronization block
     */
    public Weight[] getWeights(SearchContext searchContext) {
        if (weights != null) {
            return weights;
        }

        // This will happen only for the first segment access in the slices. After that for other segments
        // weights will be non-null and returned from above
        synchronized (this) {
            if (weights == null) {
                try {
                    final Weight[] filterWeights = new Weight[filters.length];
                    IndexSearcher contextSearcher = searchContext.searcher();
                    for (int i = 0; i < filters.length; ++i) {
                        filterWeights[i] = contextSearcher.createWeight(
                            contextSearcher.rewrite(filters[i]),
                            ScoreMode.COMPLETE_NO_SCORES,
                            1
                        );
                    }
                    weights = filterWeights;
                } catch (IOException e) {
                    throw new AggregationInitializationException("Failed to initialze filters for aggregation [" + name() + "]", e);
                }
            }
        }
        return weights;
    }

    @Override
    public Aggregator createInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        return new FiltersAggregator(
            name,
            factories,
            keys,
            () -> getWeights(searchContext),
            keyed,
            otherBucket ? otherBucketKey : null,
            searchContext,
            parent,
            cardinality,
            metadata
        );
    }

    @Override
    protected boolean supportsConcurrentSegmentSearch() {
        return true;
    }
}
