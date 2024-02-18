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

package org.opensearch.search.aggregations.support;

import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public abstract class ArrayValuesSourceAggregatorFactory extends AggregatorFactory {

    protected Map<String, ValuesSourceConfig> configs;

    public ArrayValuesSourceAggregatorFactory(
        String name,
        Map<String, ValuesSourceConfig> configs,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.configs = configs;
    }

    @Override
    public Aggregator createInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        HashMap<String, ValuesSource> valuesSources = new HashMap<>();

        for (Map.Entry<String, ValuesSourceConfig> config : configs.entrySet()) {
            ValuesSourceConfig vsc = config.getValue();
            if (vsc.hasValues()) {
                valuesSources.put(config.getKey(), vsc.getValuesSource());
            }
        }
        if (valuesSources.isEmpty()) {
            return createUnmapped(searchContext, parent, metadata);
        }
        return doCreateInternal(valuesSources, searchContext, parent, cardinality, metadata);
    }

    /**
     * Create the {@linkplain Aggregator} when none of the configured
     * fields can be resolved to a {@link ValuesSource}.
     */
    protected abstract Aggregator createUnmapped(SearchContext searchContext, Aggregator parent, Map<String, Object> metadata)
        throws IOException;

    /**
     * Create the {@linkplain Aggregator} when any of the configured
     * fields can be resolved to a {@link ValuesSource}.
     *
     * @param cardinality Upper bound of the number of {@code owningBucketOrd}s
     *                    that the {@link Aggregator} created by this method
     *                    will be asked to collect.
     */
    protected abstract Aggregator doCreateInternal(
        Map<String, ValuesSource> valuesSources,
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException;

}
