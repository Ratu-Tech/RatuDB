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

package org.opensearch.geo.search.aggregations.bucket.geogrid;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ScoreMode;
import org.opensearch.common.lease.Releasables;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.bucket.BucketsAggregator;
import org.opensearch.search.aggregations.bucket.terms.LongKeyedBucketOrds;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Aggregates data expressed as longs (for efficiency's sake) but formats results as aggregation-specific strings.
 *
 * @opensearch.api
 */
public abstract class GeoGridAggregator<T extends BaseGeoGrid> extends BucketsAggregator {

    protected final int requiredSize;
    protected final int shardSize;
    protected final ValuesSource.Numeric valuesSource;
    protected final LongKeyedBucketOrds bucketOrds;

    protected GeoGridAggregator(
        String name,
        AggregatorFactories factories,
        ValuesSource.Numeric valuesSource,
        int requiredSize,
        int shardSize,
        SearchContext aggregationContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, aggregationContext, parent, CardinalityUpperBound.MANY, metadata);
        this.valuesSource = valuesSource;
        this.requiredSize = requiredSize;
        this.shardSize = shardSize;
        bucketOrds = LongKeyedBucketOrds.build(context.bigArrays(), cardinality);
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSource != null && valuesSource.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        SortedNumericDocValues values = valuesSource.longValues(ctx);
        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();

                    long previous = Long.MAX_VALUE;
                    for (int i = 0; i < valuesCount; ++i) {
                        final long val = values.nextValue();
                        if (previous != val || i == 0) {
                            long bucketOrdinal = bucketOrds.add(owningBucketOrd, val);
                            if (bucketOrdinal < 0) { // already seen
                                bucketOrdinal = -1 - bucketOrdinal;
                                collectExistingBucket(sub, doc, bucketOrdinal);
                            } else {
                                collectBucket(sub, doc, bucketOrdinal);
                            }
                            previous = val;
                        }
                    }
                }
            }
        };
    }

    protected abstract T buildAggregation(String name, int requiredSize, List<BaseGeoGridBucket> buckets, Map<String, Object> metadata);

    /**
     * This method is used to return a re-usable instance of the bucket when building
     * the aggregation.
     * @return a new {@link BaseGeoGridBucket} implementation with empty parameters
     */
    protected abstract BaseGeoGridBucket newEmptyBucket();

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        BaseGeoGridBucket[][] topBucketsPerOrd = new BaseGeoGridBucket[owningBucketOrds.length][];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            int size = (int) Math.min(bucketOrds.bucketsInOrd(owningBucketOrds[ordIdx]), shardSize);

            BucketPriorityQueue<BaseGeoGridBucket> ordered = new BucketPriorityQueue<>(size);
            BaseGeoGridBucket spare = null;
            LongKeyedBucketOrds.BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrds[ordIdx]);
            while (ordsEnum.next()) {
                if (spare == null) {
                    spare = newEmptyBucket();
                }

                // need a special function to keep the source bucket
                // up-to-date so it can get the appropriate key
                spare.hashAsLong = ordsEnum.value();
                spare.docCount = bucketDocCount(ordsEnum.ord());
                spare.bucketOrd = ordsEnum.ord();
                spare = ordered.insertWithOverflow(spare);
            }

            topBucketsPerOrd[ordIdx] = new BaseGeoGridBucket[ordered.size()];
            for (int i = ordered.size() - 1; i >= 0; --i) {
                topBucketsPerOrd[ordIdx][i] = ordered.pop();
            }
        }
        buildSubAggsForAllBuckets(topBucketsPerOrd, b -> b.bucketOrd, (b, aggs) -> b.aggregations = aggs);
        InternalAggregation[] results = new InternalAggregation[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            results[ordIdx] = buildAggregation(name, requiredSize, Arrays.asList(topBucketsPerOrd[ordIdx]), metadata());
        }
        return results;
    }

    @Override
    public BaseGeoGrid buildEmptyAggregation() {
        return buildAggregation(name, requiredSize, Collections.emptyList(), metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
    }
}
