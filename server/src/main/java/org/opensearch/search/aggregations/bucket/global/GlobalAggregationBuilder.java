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

package org.opensearch.search.aggregations.bucket.global;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AbstractAggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories.Builder;
import org.opensearch.search.aggregations.AggregatorFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Aggregation Builder for global agg
 *
 * @opensearch.internal
 */
public class GlobalAggregationBuilder extends AbstractAggregationBuilder<GlobalAggregationBuilder> {
    public static GlobalAggregationBuilder parse(XContentParser parser, String aggregationName) throws IOException {
        parser.nextToken();
        return new GlobalAggregationBuilder(aggregationName);
    }

    public static final String NAME = "global";

    public GlobalAggregationBuilder(String name) {
        super(name);
    }

    protected GlobalAggregationBuilder(GlobalAggregationBuilder clone, Builder factoriesBuilder, Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
        return new GlobalAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Read from a stream.
     */
    public GlobalAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        // Nothing to write
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.ONE;
    }

    @Override
    protected AggregatorFactory doBuild(QueryShardContext queryShardContext, AggregatorFactory parent, Builder subFactoriesBuilder)
        throws IOException {
        return new GlobalAggregatorFactory(name, queryShardContext, parent, subFactoriesBuilder, metadata);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.endObject();
        return builder;
    }

    @Override
    public String getType() {
        return NAME;
    }
}
