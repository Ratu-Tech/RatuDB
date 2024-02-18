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

package org.opensearch.search.aggregations.bucket.nested;

import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.support.NestedScope;
import org.opensearch.search.aggregations.AbstractAggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationExecutionException;
import org.opensearch.search.aggregations.AggregatorFactories.Builder;
import org.opensearch.search.aggregations.AggregatorFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Aggregation Builder for reverse_nested agg
 *
 * @opensearch.internal
 */
public class ReverseNestedAggregationBuilder extends AbstractAggregationBuilder<ReverseNestedAggregationBuilder> {
    public static final String NAME = "reverse_nested";

    private String path;

    public ReverseNestedAggregationBuilder(String name) {
        super(name);
    }

    public ReverseNestedAggregationBuilder(ReverseNestedAggregationBuilder clone, Builder factoriesBuilder, Map<String, Object> map) {
        super(clone, factoriesBuilder, map);
        this.path = clone.path;
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
        return new ReverseNestedAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Read from a stream.
     */
    public ReverseNestedAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        path = in.readOptionalString();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalString(path);
    }

    /**
     * Set the path to use for this nested aggregation. The path must match
     * the path to a nested object in the mappings. If it is not specified
     * then this aggregation will go back to the root document.
     */
    public ReverseNestedAggregationBuilder path(String path) {
        if (path == null) {
            throw new IllegalArgumentException("[path] must not be null: [" + name + "]");
        }
        this.path = path;
        return this;
    }

    /**
     * Get the path to use for this nested aggregation.
     */
    public String path() {
        return path;
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.ONE;
    }

    @Override
    protected AggregatorFactory doBuild(QueryShardContext queryShardContext, AggregatorFactory parent, Builder subFactoriesBuilder)
        throws IOException {
        if (findNestedAggregatorFactory(parent) == null) {
            throw new IllegalArgumentException("Reverse nested aggregation [" + name + "] can only be used inside a [nested] aggregation");
        }

        ObjectMapper parentObjectMapper = null;
        if (path != null) {
            parentObjectMapper = queryShardContext.getObjectMapper(path);
            if (parentObjectMapper == null) {
                return new ReverseNestedAggregatorFactory(name, true, null, queryShardContext, parent, subFactoriesBuilder, metadata);
            }
            if (parentObjectMapper.nested().isNested() == false) {
                throw new AggregationExecutionException("[reverse_nested] nested path [" + path + "] is not nested");
            }
        }

        NestedScope nestedScope = queryShardContext.nestedScope();
        try {
            nestedScope.nextLevel(parentObjectMapper);
            return new ReverseNestedAggregatorFactory(
                name,
                false,
                parentObjectMapper,
                queryShardContext,
                parent,
                subFactoriesBuilder,
                metadata
            );
        } finally {
            nestedScope.previousLevel();
        }
    }

    private static NestedAggregatorFactory findNestedAggregatorFactory(AggregatorFactory parent) {
        if (parent == null) {
            return null;
        } else if (parent instanceof NestedAggregatorFactory) {
            return (NestedAggregatorFactory) parent;
        } else {
            return findNestedAggregatorFactory(parent.getParent());
        }
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (path != null) {
            builder.field(ReverseNestedAggregator.PATH_FIELD.getPreferredName(), path);
        }
        builder.endObject();
        return builder;
    }

    public static ReverseNestedAggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        String path = null;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("path".equals(currentFieldName)) {
                    path = parser.text();
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "]."
                    );
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "Unexpected token " + token + " in [" + aggregationName + "].");
            }
        }

        ReverseNestedAggregationBuilder factory = new ReverseNestedAggregationBuilder(aggregationName);
        if (path != null) {
            factory.path(path);
        }
        return factory;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), path);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        ReverseNestedAggregationBuilder other = (ReverseNestedAggregationBuilder) obj;
        return Objects.equals(path, other.path);
    }

    @Override
    public String getType() {
        return NAME;
    }
}
