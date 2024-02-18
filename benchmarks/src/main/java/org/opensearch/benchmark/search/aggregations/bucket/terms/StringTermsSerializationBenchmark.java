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

package org.opensearch.benchmark.search.aggregations.bucket.terms;

import org.apache.lucene.util.BytesRef;
import org.opensearch.common.io.stream.DelayableWriteable;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Fork(2)
@Warmup(iterations = 10)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class StringTermsSerializationBenchmark {
    private static final NamedWriteableRegistry REGISTRY = new NamedWriteableRegistry(
        List.of(new NamedWriteableRegistry.Entry(InternalAggregation.class, StringTerms.NAME, StringTerms::new))
    );
    @Param(value = { "1000" })
    private int buckets;

    private DelayableWriteable<InternalAggregations> results;

    @Setup
    public void initResults() {
        results = DelayableWriteable.referencing(InternalAggregations.from(List.of(newTerms(true))));
    }

    private StringTerms newTerms(boolean withNested) {
        List<StringTerms.Bucket> resultBuckets = new ArrayList<>(buckets);
        for (int i = 0; i < buckets; i++) {
            InternalAggregations inner = withNested ? InternalAggregations.from(List.of(newTerms(false))) : InternalAggregations.EMPTY;
            resultBuckets.add(new StringTerms.Bucket(new BytesRef("test" + i), i, inner, false, 0, DocValueFormat.RAW));
        }
        return new StringTerms(
            "test",
            BucketOrder.key(true),
            BucketOrder.key(true),
            null,
            DocValueFormat.RAW,
            buckets,
            false,
            100000,
            resultBuckets,
            0,
            new TermsAggregator.BucketCountThresholds(1, 0, buckets, buckets)
        );
    }

    @Benchmark
    public DelayableWriteable<InternalAggregations> serialize() {
        return results.asSerialized(InternalAggregations::readFrom, REGISTRY);
    }
}
