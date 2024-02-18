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

package org.opensearch.search.aggregations.bucket.sampler;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories.Builder;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;
import org.opensearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Aggregation Builder for diversified_sampler agg
 *
 * @opensearch.internal
 */
public class DiversifiedAggregationBuilder extends ValuesSourceAggregationBuilder<DiversifiedAggregationBuilder> {
    public static final String NAME = "diversified_sampler";
    public static final ValuesSourceRegistry.RegistryKey<DiversifiedAggregatorSupplier> REGISTRY_KEY =
        new ValuesSourceRegistry.RegistryKey<>(NAME, DiversifiedAggregatorSupplier.class);

    public static final int MAX_DOCS_PER_VALUE_DEFAULT = 1;

    public static final ObjectParser<DiversifiedAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        NAME,
        DiversifiedAggregationBuilder::new
    );
    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, false, false);
        PARSER.declareInt(DiversifiedAggregationBuilder::shardSize, SamplerAggregator.SHARD_SIZE_FIELD);
        PARSER.declareInt(DiversifiedAggregationBuilder::maxDocsPerValue, SamplerAggregator.MAX_DOCS_PER_VALUE_FIELD);
        PARSER.declareString(DiversifiedAggregationBuilder::executionHint, SamplerAggregator.EXECUTION_HINT_FIELD);
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        DiversifiedAggregatorFactory.registerAggregators(builder);
    }

    private int shardSize = SamplerAggregationBuilder.DEFAULT_SHARD_SAMPLE_SIZE;
    private int maxDocsPerValue = MAX_DOCS_PER_VALUE_DEFAULT;
    private String executionHint = null;

    public DiversifiedAggregationBuilder(String name) {
        super(name);
    }

    protected DiversifiedAggregationBuilder(DiversifiedAggregationBuilder clone, Builder factoriesBuilder, Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
        this.shardSize = clone.shardSize;
        this.maxDocsPerValue = clone.maxDocsPerValue;
        this.executionHint = clone.executionHint;
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.BYTES;
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
        return new DiversifiedAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Read from a stream.
     */
    public DiversifiedAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        shardSize = in.readVInt();
        maxDocsPerValue = in.readVInt();
        executionHint = in.readOptionalString();
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(shardSize);
        out.writeVInt(maxDocsPerValue);
        out.writeOptionalString(executionHint);
    }

    /**
     * Set the max num docs to be returned from each shard.
     */
    public DiversifiedAggregationBuilder shardSize(int shardSize) {
        if (shardSize < 0) {
            throw new IllegalArgumentException(
                "[shardSize] must be greater than or equal to 0. Found [" + shardSize + "] in [" + name + "]"
            );
        }
        this.shardSize = shardSize;
        return this;
    }

    /**
     * Get the max num docs to be returned from each shard.
     */
    public int shardSize() {
        return shardSize;
    }

    /**
     * Set the max num docs to be returned per value.
     */
    public DiversifiedAggregationBuilder maxDocsPerValue(int maxDocsPerValue) {
        if (maxDocsPerValue < 0) {
            throw new IllegalArgumentException(
                "[maxDocsPerValue] must be greater than or equal to 0. Found [" + maxDocsPerValue + "] in [" + name + "]"
            );
        }
        this.maxDocsPerValue = maxDocsPerValue;
        return this;
    }

    /**
     * Get the max num docs to be returned per value.
     */
    public int maxDocsPerValue() {
        return maxDocsPerValue;
    }

    /**
     * Set the execution hint.
     */
    public DiversifiedAggregationBuilder executionHint(String executionHint) {
        this.executionHint = executionHint;
        return this;
    }

    /**
     * Get the execution hint.
     */
    public String executionHint() {
        return executionHint;
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.ONE;
    }

    @Override
    protected ValuesSourceAggregatorFactory innerBuild(
        QueryShardContext queryShardContext,
        ValuesSourceConfig config,
        AggregatorFactory parent,
        Builder subFactoriesBuilder
    ) throws IOException {
        return new DiversifiedAggregatorFactory(
            name,
            config,
            shardSize,
            maxDocsPerValue,
            executionHint,
            queryShardContext,
            parent,
            subFactoriesBuilder,
            metadata
        );
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(SamplerAggregator.SHARD_SIZE_FIELD.getPreferredName(), shardSize);
        builder.field(SamplerAggregator.MAX_DOCS_PER_VALUE_FIELD.getPreferredName(), maxDocsPerValue);
        if (executionHint != null) {
            builder.field(SamplerAggregator.EXECUTION_HINT_FIELD.getPreferredName(), executionHint);
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), shardSize, maxDocsPerValue, executionHint);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        DiversifiedAggregationBuilder other = (DiversifiedAggregationBuilder) obj;
        return Objects.equals(shardSize, other.shardSize)
            && Objects.equals(maxDocsPerValue, other.maxDocsPerValue)
            && Objects.equals(executionHint, other.executionHint);
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    protected ValuesSourceRegistry.RegistryKey<?> getRegistryKey() {
        return REGISTRY_KEY;
    }
}
