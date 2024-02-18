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

package org.opensearch.search.aggregations.bucket;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.geometry.utils.Geohash;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.opensearch.search.aggregations.bucket.filter.Filter;
import org.opensearch.search.aggregations.bucket.global.Global;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.bucket.missing.Missing;
import org.opensearch.search.aggregations.bucket.nested.Nested;
import org.opensearch.search.aggregations.bucket.range.Range;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedOpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.opensearch.search.aggregations.AggregationBuilders.dateRange;
import static org.opensearch.search.aggregations.AggregationBuilders.filter;
import static org.opensearch.search.aggregations.AggregationBuilders.global;
import static org.opensearch.search.aggregations.AggregationBuilders.histogram;
import static org.opensearch.search.aggregations.AggregationBuilders.ipRange;
import static org.opensearch.search.aggregations.AggregationBuilders.missing;
import static org.opensearch.search.aggregations.AggregationBuilders.nested;
import static org.opensearch.search.aggregations.AggregationBuilders.range;
import static org.opensearch.search.aggregations.AggregationBuilders.terms;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests making sure that the reduce is propagated to all aggregations in the hierarchy when executing on a single shard
 * These tests are based on the date histogram in combination of min_doc_count=0. In order for the date histogram to
 * compute empty buckets, its {@code reduce()} method must be called. So by adding the date histogram under other buckets,
 * we can make sure that the reduce is properly propagated by checking that empty buckets were created.
 */
@OpenSearchIntegTestCase.SuiteScopeTestCase
public class ShardReduceIT extends ParameterizedOpenSearchIntegTestCase {

    public ShardReduceIT(Settings dynamicSettings) {
        super(dynamicSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.CONCURRENT_SEGMENT_SEARCH, "true").build();
    }

    private IndexRequestBuilder indexDoc(String date, int value) throws Exception {
        return client().prepareIndex("idx")
            .setSource(
                jsonBuilder().startObject()
                    .field("value", value)
                    .field("ip", "10.0.0." + value)
                    .field("location", Geohash.stringEncode(5, 52, Geohash.PRECISION))
                    .field("date", date)
                    .field("term-l", 1)
                    .field("term-d", 1.5)
                    .field("term-s", "term")
                    .startObject("nested")
                    .field("date", date)
                    .endObject()
                    .endObject()
            );
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        assertAcked(
            prepareCreate("idx").setMapping(
                "nested",
                "type=nested",
                "ip",
                "type=ip",
                "location",
                "type=geo_point",
                "term-s",
                "type=keyword"
            )
        );

        indexRandom(true, indexDoc("2014-01-01", 1), indexDoc("2014-01-02", 2), indexDoc("2014-01-04", 3));
        ensureSearchable();
    }

    public void testGlobal() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(
                global("global").subAggregation(
                    dateHistogram("histo").field("date").dateHistogramInterval(DateHistogramInterval.DAY).minDocCount(0)
                )
            )
            .get();

        assertSearchResponse(response);

        Global global = response.getAggregations().get("global");
        Histogram histo = global.getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), equalTo(4));
    }

    public void testFilter() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(
                filter("filter", QueryBuilders.matchAllQuery()).subAggregation(
                    dateHistogram("histo").field("date").dateHistogramInterval(DateHistogramInterval.DAY).minDocCount(0)
                )
            )
            .get();

        assertSearchResponse(response);

        Filter filter = response.getAggregations().get("filter");
        Histogram histo = filter.getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), equalTo(4));
    }

    public void testMissing() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(
                missing("missing").field("foobar")
                    .subAggregation(dateHistogram("histo").field("date").dateHistogramInterval(DateHistogramInterval.DAY).minDocCount(0))
            )
            .get();

        assertSearchResponse(response);

        Missing missing = response.getAggregations().get("missing");
        Histogram histo = missing.getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), equalTo(4));
    }

    public void testGlobalWithFilterWithMissing() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(
                global("global").subAggregation(
                    filter("filter", QueryBuilders.matchAllQuery()).subAggregation(
                        missing("missing").field("foobar")
                            .subAggregation(
                                dateHistogram("histo").field("date").dateHistogramInterval(DateHistogramInterval.DAY).minDocCount(0)
                            )
                    )
                )
            )
            .get();

        assertSearchResponse(response);

        Global global = response.getAggregations().get("global");
        Filter filter = global.getAggregations().get("filter");
        Missing missing = filter.getAggregations().get("missing");
        Histogram histo = missing.getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), equalTo(4));
    }

    public void testNested() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(
                nested("nested", "nested").subAggregation(
                    dateHistogram("histo").field("nested.date").dateHistogramInterval(DateHistogramInterval.DAY).minDocCount(0)
                )
            )
            .get();

        assertSearchResponse(response);

        Nested nested = response.getAggregations().get("nested");
        Histogram histo = nested.getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), equalTo(4));
    }

    public void testStringTerms() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(
                terms("terms").field("term-s")
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .subAggregation(dateHistogram("histo").field("date").dateHistogramInterval(DateHistogramInterval.DAY).minDocCount(0))
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        Histogram histo = terms.getBucketByKey("term").getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), equalTo(4));
    }

    public void testLongTerms() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(
                terms("terms").field("term-l")
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .subAggregation(dateHistogram("histo").field("date").dateHistogramInterval(DateHistogramInterval.DAY).minDocCount(0))
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        Histogram histo = terms.getBucketByKey("1").getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), equalTo(4));
    }

    public void testDoubleTerms() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(
                terms("terms").field("term-d")
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .subAggregation(dateHistogram("histo").field("date").dateHistogramInterval(DateHistogramInterval.DAY).minDocCount(0))
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        Histogram histo = terms.getBucketByKey("1.5").getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), equalTo(4));
    }

    public void testRange() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(
                range("range").field("value")
                    .addRange("r1", 0, 10)
                    .subAggregation(dateHistogram("histo").field("date").dateHistogramInterval(DateHistogramInterval.DAY).minDocCount(0))
            )
            .get();

        assertSearchResponse(response);

        Range range = response.getAggregations().get("range");
        Histogram histo = range.getBuckets().get(0).getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), equalTo(4));
    }

    public void testDateRange() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(
                dateRange("range").field("date")
                    .addRange("r1", "2014-01-01", "2014-01-10")
                    .subAggregation(dateHistogram("histo").field("date").dateHistogramInterval(DateHistogramInterval.DAY).minDocCount(0))
            )
            .get();

        assertSearchResponse(response);

        Range range = response.getAggregations().get("range");
        Histogram histo = range.getBuckets().get(0).getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), equalTo(4));
    }

    public void testIpRange() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(
                ipRange("range").field("ip")
                    .addRange("r1", "10.0.0.1", "10.0.0.10")
                    .subAggregation(dateHistogram("histo").field("date").dateHistogramInterval(DateHistogramInterval.DAY).minDocCount(0))
            )
            .get();

        assertSearchResponse(response);

        Range range = response.getAggregations().get("range");
        Histogram histo = range.getBuckets().get(0).getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), equalTo(4));
    }

    public void testHistogram() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(
                histogram("topHisto").field("value")
                    .interval(5)
                    .subAggregation(dateHistogram("histo").field("date").dateHistogramInterval(DateHistogramInterval.DAY).minDocCount(0))
            )
            .get();

        assertSearchResponse(response);

        Histogram topHisto = response.getAggregations().get("topHisto");
        Histogram histo = topHisto.getBuckets().get(0).getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), equalTo(4));
    }

    public void testDateHistogram() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(
                dateHistogram("topHisto").field("date")
                    .dateHistogramInterval(DateHistogramInterval.MONTH)
                    .subAggregation(dateHistogram("histo").field("date").dateHistogramInterval(DateHistogramInterval.DAY).minDocCount(0))
            )
            .get();

        assertSearchResponse(response);

        Histogram topHisto = response.getAggregations().get("topHisto");
        Histogram histo = topHisto.getBuckets().iterator().next().getAggregations().get("histo");
        assertThat(histo.getBuckets().size(), equalTo(4));

    }

}
