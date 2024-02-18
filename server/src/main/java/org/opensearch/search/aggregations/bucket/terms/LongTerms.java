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

package org.opensearch.search.aggregations.bucket.terms;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Result of the {@link TermsAggregator} when the field is some kind of whole number like a integer, long, or a date.
 *
 * @opensearch.internal
 */
public class LongTerms extends InternalMappedTerms<LongTerms, LongTerms.Bucket> {
    public static final String NAME = "lterms";

    /**
     * Bucket for long terms
     *
     * @opensearch.internal
     */
    public static class Bucket extends InternalTerms.Bucket<Bucket> {
        long term;

        public Bucket(
            long term,
            long docCount,
            InternalAggregations aggregations,
            boolean showDocCountError,
            long docCountError,
            DocValueFormat format
        ) {
            super(docCount, aggregations, showDocCountError, docCountError, format);
            this.term = term;
        }

        /**
         * Read from a stream.
         */
        public Bucket(StreamInput in, DocValueFormat format, boolean showDocCountError) throws IOException {
            super(in, format, showDocCountError);
            term = in.readLong();
        }

        @Override
        protected void writeTermTo(StreamOutput out) throws IOException {
            out.writeLong(term);
        }

        @Override
        public String getKeyAsString() {
            return format.format(term).toString();
        }

        @Override
        public Object getKey() {
            if (format == DocValueFormat.UNSIGNED_LONG_SHIFTED) {
                return format.format(term);
            } else {
                return term;
            }
        }

        @Override
        public Number getKeyAsNumber() {
            if (format == DocValueFormat.UNSIGNED_LONG_SHIFTED) {
                return (Number) format.format(term);
            } else {
                return term;
            }
        }

        @Override
        public int compareKey(Bucket other) {
            return Long.compare(term, other.term);
        }

        @Override
        protected final XContentBuilder keyToXContent(XContentBuilder builder) throws IOException {
            if (format == DocValueFormat.UNSIGNED_LONG_SHIFTED) {
                builder.field(CommonFields.KEY.getPreferredName(), format.format(term));
            } else {
                builder.field(CommonFields.KEY.getPreferredName(), term);
            }
            if (format != DocValueFormat.RAW && format != DocValueFormat.UNSIGNED_LONG_SHIFTED) {
                builder.field(CommonFields.KEY_AS_STRING.getPreferredName(), format.format(term).toString());
            }
            return builder;
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj) && Objects.equals(term, ((Bucket) obj).term);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), term);
        }
    }

    public LongTerms(
        String name,
        BucketOrder reduceOrder,
        BucketOrder order,
        Map<String, Object> metadata,
        DocValueFormat format,
        int shardSize,
        boolean showTermDocCountError,
        long otherDocCount,
        List<Bucket> buckets,
        long docCountError,
        TermsAggregator.BucketCountThresholds bucketCountThresholds
    ) {
        super(
            name,
            reduceOrder,
            order,
            metadata,
            format,
            shardSize,
            showTermDocCountError,
            otherDocCount,
            buckets,
            docCountError,
            bucketCountThresholds
        );
    }

    /**
     * Read from a stream.
     */
    public LongTerms(StreamInput in) throws IOException {
        super(in, Bucket::new);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public LongTerms create(List<Bucket> buckets) {
        return new LongTerms(
            name,
            reduceOrder,
            order,
            metadata,
            format,
            shardSize,
            showTermDocCountError,
            otherDocCount,
            buckets,
            docCountError,
            bucketCountThresholds
        );
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(
            prototype.term,
            prototype.docCount,
            aggregations,
            prototype.showDocCountError,
            prototype.docCountError,
            prototype.format
        );
    }

    @Override
    protected LongTerms create(String name, List<Bucket> buckets, BucketOrder reduceOrder, long docCountError, long otherDocCount) {
        return new LongTerms(
            name,
            reduceOrder,
            order,
            getMetadata(),
            format,
            shardSize,
            showTermDocCountError,
            otherDocCount,
            buckets,
            docCountError,
            bucketCountThresholds
        );
    }

    @Override
    protected Bucket[] createBucketsArray(int size) {
        return new Bucket[size];
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        boolean unsignedLongFormat = false;
        boolean rawFormat = false;
        for (InternalAggregation agg : aggregations) {
            if (agg instanceof DoubleTerms) {
                return agg.reduce(aggregations, reduceContext);
            }
            if (agg instanceof LongTerms) {
                if (((LongTerms) agg).format == DocValueFormat.RAW) {
                    rawFormat = true;
                } else if (((LongTerms) agg).format == DocValueFormat.UNSIGNED_LONG_SHIFTED) {
                    unsignedLongFormat = true;
                } else if (((LongTerms) agg).format == DocValueFormat.UNSIGNED_LONG) {
                    unsignedLongFormat = true;
                }
            }
        }
        if (rawFormat && unsignedLongFormat) { // if we have mixed formats, convert results to double format
            List<InternalAggregation> newAggs = new ArrayList<>(aggregations.size());
            for (InternalAggregation agg : aggregations) {
                if (agg instanceof LongTerms) {
                    DoubleTerms dTerms = LongTerms.convertLongTermsToDouble((LongTerms) agg, format);
                    newAggs.add(dTerms);
                } else {
                    newAggs.add(agg);
                }
            }
            return newAggs.get(0).reduce(newAggs, reduceContext);
        }
        return super.reduce(aggregations, reduceContext);
    }

    @Override
    Bucket createBucket(long docCount, InternalAggregations aggs, long docCountError, LongTerms.Bucket prototype) {
        return new Bucket(prototype.term, docCount, aggs, prototype.showDocCountError, docCountError, format);
    }

    /**
     * Converts a {@link LongTerms} into a {@link DoubleTerms}, returning the value of the specified long terms as doubles.
     */
    static DoubleTerms convertLongTermsToDouble(LongTerms longTerms, DocValueFormat decimalFormat) {
        List<LongTerms.Bucket> buckets = longTerms.getBuckets();
        List<DoubleTerms.Bucket> newBuckets = new ArrayList<>();
        for (Terms.Bucket bucket : buckets) {
            newBuckets.add(
                new DoubleTerms.Bucket(
                    bucket.getKeyAsNumber().doubleValue(),
                    bucket.getDocCount(),
                    (InternalAggregations) bucket.getAggregations(),
                    longTerms.showTermDocCountError,
                    longTerms.showTermDocCountError ? bucket.getDocCountError() : 0,
                    decimalFormat
                )
            );
        }
        return new DoubleTerms(
            longTerms.getName(),
            longTerms.reduceOrder,
            longTerms.order,
            longTerms.metadata,
            longTerms.format,
            longTerms.shardSize,
            longTerms.showTermDocCountError,
            longTerms.otherDocCount,
            newBuckets,
            longTerms.docCountError,
            longTerms.bucketCountThresholds
        );
    }
}
