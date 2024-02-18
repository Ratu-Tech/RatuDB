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

import org.opensearch.script.BucketAggregationScript;
import org.opensearch.script.Script;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregation.ReduceContext;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.InternalMultiBucketAggregation;
import org.opensearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.opensearch.search.aggregations.pipeline.BucketHelpers.resolveBucketValue;

/**
 * Aggregate all docs into a bucket defined by a script
 *
 * @opensearch.internal
 */
public class BucketScriptPipelineAggregator extends PipelineAggregator {
    private final DocValueFormat formatter;
    private final GapPolicy gapPolicy;
    private final Script script;
    private final Map<String, String> bucketsPathsMap;

    BucketScriptPipelineAggregator(
        String name,
        Map<String, String> bucketsPathsMap,
        Script script,
        DocValueFormat formatter,
        GapPolicy gapPolicy,
        Map<String, Object> metadata
    ) {
        super(name, bucketsPathsMap.values().toArray(new String[0]), metadata);
        this.bucketsPathsMap = bucketsPathsMap;
        this.script = script;
        this.formatter = formatter;
        this.gapPolicy = gapPolicy;
    }

    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext) {
        InternalMultiBucketAggregation<InternalMultiBucketAggregation, InternalMultiBucketAggregation.InternalBucket> originalAgg =
            (InternalMultiBucketAggregation<InternalMultiBucketAggregation, InternalMultiBucketAggregation.InternalBucket>) aggregation;
        List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = originalAgg.getBuckets();

        BucketAggregationScript.Factory factory = reduceContext.scriptService().compile(script, BucketAggregationScript.CONTEXT);
        List<InternalMultiBucketAggregation.InternalBucket> newBuckets = new ArrayList<>();
        for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
            Map<String, Object> vars = new HashMap<>();
            if (script.getParams() != null) {
                vars.putAll(script.getParams());
            }
            boolean skipBucket = false;
            for (Map.Entry<String, String> entry : bucketsPathsMap.entrySet()) {
                String varName = entry.getKey();
                String bucketsPath = entry.getValue();
                Double value = resolveBucketValue(originalAgg, bucket, bucketsPath, gapPolicy);
                if (GapPolicy.SKIP == gapPolicy && (value == null || Double.isNaN(value))) {
                    skipBucket = true;
                    break;
                }
                vars.put(varName, value);
            }
            if (skipBucket) {
                newBuckets.add(bucket);
            } else {
                Number returned = factory.newInstance(vars).execute();
                if (returned == null) {
                    newBuckets.add(bucket);
                } else {
                    final List<InternalAggregation> aggs = StreamSupport.stream(bucket.getAggregations().spliterator(), false)
                        .map((p) -> (InternalAggregation) p)
                        .collect(Collectors.toList());

                    InternalSimpleValue simpleValue = new InternalSimpleValue(name(), returned.doubleValue(), formatter, metadata());
                    aggs.add(simpleValue);
                    InternalMultiBucketAggregation.InternalBucket newBucket = originalAgg.createBucket(
                        InternalAggregations.from(aggs),
                        bucket
                    );
                    newBuckets.add(newBucket);
                }
            }
        }
        return originalAgg.create(newBuckets);
    }
}
