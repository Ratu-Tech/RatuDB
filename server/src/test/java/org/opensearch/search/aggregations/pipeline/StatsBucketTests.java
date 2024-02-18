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

package org.opensearch.search.aggregations.pipeline;

import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.support.ValueType;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class StatsBucketTests extends AbstractBucketMetricsTestCase<StatsBucketPipelineAggregationBuilder> {

    @Override
    protected StatsBucketPipelineAggregationBuilder doCreateTestAggregatorFactory(String name, String bucketsPath) {
        return new StatsBucketPipelineAggregationBuilder(name, bucketsPath);
    }

    public void testValidate() {
        AggregationBuilder singleBucketAgg = new GlobalAggregationBuilder("global");
        AggregationBuilder multiBucketAgg = new TermsAggregationBuilder("terms").userValueTypeHint(ValueType.STRING);
        final Set<AggregationBuilder> aggBuilders = new HashSet<>();
        aggBuilders.add(singleBucketAgg);
        aggBuilders.add(multiBucketAgg);

        // First try to point to a non-existent agg
        assertThat(
            validate(aggBuilders, new StatsBucketPipelineAggregationBuilder("name", "invalid_agg>metric")),
            equalTo(
                "Validation Failed: 1: "
                    + PipelineAggregator.Parser.BUCKETS_PATH.getPreferredName()
                    + " aggregation does not exist for aggregation [name]: invalid_agg>metric;"
            )
        );

        // Now try to point to a single bucket agg
        assertThat(
            validate(aggBuilders, new StatsBucketPipelineAggregationBuilder("name", "global>metric")),
            equalTo(
                "Validation Failed: 1: The first aggregation in "
                    + PipelineAggregator.Parser.BUCKETS_PATH.getPreferredName()
                    + " must be a multi-bucket aggregation for aggregation [name] found :"
                    + GlobalAggregationBuilder.class.getName()
                    + " for buckets path: global>metric;"
            )
        );

        // Now try to point to a valid multi-bucket agg
        assertThat(validate(aggBuilders, new StatsBucketPipelineAggregationBuilder("name", "terms>metric")), nullValue());
    }

}
