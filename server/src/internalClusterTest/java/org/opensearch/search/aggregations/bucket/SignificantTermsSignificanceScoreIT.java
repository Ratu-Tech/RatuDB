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

import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.script.MockScriptPlugin;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.filter.InternalFilter;
import org.opensearch.search.aggregations.bucket.terms.SignificantTerms;
import org.opensearch.search.aggregations.bucket.terms.SignificantTermsAggregatorFactory;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.bucket.terms.heuristic.ChiSquare;
import org.opensearch.search.aggregations.bucket.terms.heuristic.GND;
import org.opensearch.search.aggregations.bucket.terms.heuristic.MutualInformation;
import org.opensearch.search.aggregations.bucket.terms.heuristic.ScriptHeuristic;
import org.opensearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedOpenSearchIntegTestCase;
import org.opensearch.test.search.aggregations.bucket.SharedSignificantTermsTestMethods;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.search.aggregations.AggregationBuilders.filter;
import static org.opensearch.search.aggregations.AggregationBuilders.significantTerms;
import static org.opensearch.search.aggregations.AggregationBuilders.significantText;
import static org.opensearch.search.aggregations.AggregationBuilders.terms;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE)
public class SignificantTermsSignificanceScoreIT extends ParameterizedOpenSearchIntegTestCase {

    static final String INDEX_NAME = "testidx";
    static final String TEXT_FIELD = "text";
    static final String CLASS_FIELD = "class";

    public SignificantTermsSignificanceScoreIT(Settings dynamicSettings) {
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

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(TestScriptPlugin.class);
    }

    public String randomExecutionHint() {
        return randomBoolean() ? null : randomFrom(SignificantTermsAggregatorFactory.ExecutionMode.values()).toString();
    }

    public static class TestScriptPlugin extends MockScriptPlugin implements SearchPlugin {
        @Override
        public Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
            scripts.put("script_with_params", params -> {
                double factor = ((Number) params.get("param")).doubleValue();
                return factor * (longValue(params.get("_subset_freq")) + longValue(params.get("_subset_size")) + longValue(
                    params.get("_superset_freq")
                ) + longValue(params.get("_superset_size"))) / factor;
            });
            scripts.put(
                "script_no_params",
                params -> longValue(params.get("_subset_freq")) + longValue(params.get("_subset_size")) + longValue(
                    params.get("_superset_freq")
                ) + longValue(params.get("_superset_size"))
            );
            return scripts;
        }

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> nonDeterministicPluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("Math.random()", vars -> SignificantTermsSignificanceScoreIT.randomDouble());

            return scripts;
        }

        private static long longValue(Object value) {
            return ((ScriptHeuristic.LongAccessor) value).longValue();
        }
    }

    public void testXContentResponse() throws Exception {
        String type = randomBoolean() ? "text" : "long";
        String settings = "{\"index.number_of_shards\": 1, \"index.number_of_replicas\": 0}";
        SharedSignificantTermsTestMethods.index01Docs(type, settings, this);

        SearchRequestBuilder request;
        if ("text".equals(type) && randomBoolean()) {
            // Use significant_text on text fields but occasionally run with alternative of
            // significant_terms on legacy fieldData=true too.
            request = client().prepareSearch(INDEX_NAME)
                .addAggregation(terms("class").field(CLASS_FIELD).subAggregation(significantText("sig_terms", TEXT_FIELD)));
        } else {
            request = client().prepareSearch(INDEX_NAME)
                .addAggregation(terms("class").field(CLASS_FIELD).subAggregation(significantTerms("sig_terms").field(TEXT_FIELD)));
        }

        SearchResponse response = request.get();

        assertSearchResponse(response);
        StringTerms classes = response.getAggregations().get("class");
        assertThat(classes.getBuckets().size(), equalTo(2));
        for (Terms.Bucket classBucket : classes.getBuckets()) {
            Map<String, Aggregation> aggs = classBucket.getAggregations().asMap();
            assertTrue(aggs.containsKey("sig_terms"));
            SignificantTerms agg = (SignificantTerms) aggs.get("sig_terms");
            assertThat(agg.getBuckets().size(), equalTo(1));
            String term = agg.iterator().next().getKeyAsString();
            String classTerm = classBucket.getKeyAsString();
            assertTrue(term.equals(classTerm));
        }

        XContentBuilder responseBuilder = XContentFactory.jsonBuilder();
        responseBuilder.startObject();
        classes.toXContent(responseBuilder, ToXContent.EMPTY_PARAMS);
        responseBuilder.endObject();

        String result = "{\"class\":{\"doc_count_error_upper_bound\":0,\"sum_other_doc_count\":0,"
            + "\"buckets\":["
            + "{"
            + "\"key\":\"0\","
            + "\"doc_count\":4,"
            + "\"sig_terms\":{"
            + "\"doc_count\":4,"
            + "\"bg_count\":7,"
            + "\"buckets\":["
            + "{"
            + "\"key\":"
            + (type.equals("long") ? "0," : "\"0\",")
            + "\"doc_count\":4,"
            + "\"score\":0.39999999999999997,"
            + "\"bg_count\":5"
            + "}"
            + "]"
            + "}"
            + "},"
            + "{"
            + "\"key\":\"1\","
            + "\"doc_count\":3,"
            + "\"sig_terms\":{"
            + "\"doc_count\":3,"
            + "\"bg_count\":7,"
            + "\"buckets\":["
            + "{"
            + "\"key\":"
            + (type.equals("long") ? "1," : "\"1\",")
            + "\"doc_count\":3,"
            + "\"score\":0.75,"
            + "\"bg_count\":4"
            + "}]}}]}}";
        assertThat(responseBuilder.toString(), equalTo(result));

    }

    public void testConsistencyWithDifferentShardCounts() throws Exception {
        // The purpose of this test is to validate that the aggregation results do not change with shard count.
        // bg_count for significant term agg is summed up across shards, so in this test we compare a 1 shard and 2 shard search request
        String type = randomBoolean() ? "text" : "long";
        String settings = "{\"index.number_of_shards\": 1, \"index.number_of_replicas\": 0}";
        SharedSignificantTermsTestMethods.index01Docs(type, settings, this);

        SearchRequestBuilder request = client().prepareSearch(INDEX_NAME)
            .setQuery(new TermQueryBuilder(CLASS_FIELD, "0"))
            .addAggregation((significantTerms("sig_terms").field(TEXT_FIELD)));

        SearchResponse response1 = request.get();

        assertAcked(client().admin().indices().delete(new DeleteIndexRequest("*")).get());

        settings = "{\"index.number_of_shards\": 2, \"index.number_of_replicas\": 0}";
        // We use a custom routing strategy here to ensure that each shard will have at least 1 bucket.
        // If there are no buckets collected for a shard, then that will affect the scoring and bg_count and our assertion will not be
        // valid.
        SharedSignificantTermsTestMethods.index01DocsWithRouting(type, settings, this);
        SearchResponse response2 = request.get();

        assertEquals(
            response1.getAggregations().asMap().get("sig_terms").toString(),
            response2.getAggregations().asMap().get("sig_terms").toString()
        );
    }

    public void testPopularTermManyDeletedDocs() throws Exception {
        String settings = "{\"index.number_of_shards\": 1, \"index.number_of_replicas\": 0}";
        assertAcked(
            prepareCreate(INDEX_NAME).setSettings(settings, MediaTypeRegistry.JSON)
                .setMapping("text", "type=keyword", CLASS_FIELD, "type=keyword")
        );
        String[] cat1v1 = { "constant", "one" };
        String[] cat1v2 = { "constant", "uno" };
        String[] cat2v1 = { "constant", "two" };
        String[] cat2v2 = { "constant", "duo" };
        List<IndexRequestBuilder> indexRequestBuilderList = new ArrayList<>();
        indexRequestBuilderList.add(client().prepareIndex(INDEX_NAME).setId("1").setSource(TEXT_FIELD, cat1v1, CLASS_FIELD, "1"));
        indexRequestBuilderList.add(client().prepareIndex(INDEX_NAME).setId("2").setSource(TEXT_FIELD, cat1v2, CLASS_FIELD, "1"));
        indexRequestBuilderList.add(client().prepareIndex(INDEX_NAME).setId("3").setSource(TEXT_FIELD, cat2v1, CLASS_FIELD, "2"));
        indexRequestBuilderList.add(client().prepareIndex(INDEX_NAME).setId("4").setSource(TEXT_FIELD, cat2v2, CLASS_FIELD, "2"));
        indexRandom(true, false, indexRequestBuilderList);

        // Now create some holes in the index with selective deletes caused by updates.
        // This is the scenario that caused this issue https://github.com/elastic/elasticsearch/issues/7951
        // Scoring algorithms throw exceptions if term docFreqs exceed the reported size of the index
        // from which they are taken so need to make sure this doesn't happen.
        String[] text = cat1v1;
        indexRequestBuilderList.clear();
        for (int i = 0; i < 50; i++) {
            text = text == cat1v2 ? cat1v1 : cat1v2;
            indexRequestBuilderList.add(client().prepareIndex(INDEX_NAME).setId("1").setSource(TEXT_FIELD, text, CLASS_FIELD, "1"));
        }
        indexRandom(true, false, indexRequestBuilderList);

        SearchRequestBuilder request;
        if (randomBoolean()) {
            request = client().prepareSearch(INDEX_NAME)
                .addAggregation(
                    terms("class").field(CLASS_FIELD).subAggregation(significantTerms("sig_terms").field(TEXT_FIELD).minDocCount(1))
                );
        } else {
            request = client().prepareSearch(INDEX_NAME)
                .addAggregation(terms("class").field(CLASS_FIELD).subAggregation(significantText("sig_terms", TEXT_FIELD).minDocCount(1)));
        }

        request.get();

    }

    public void testBackgroundVsSeparateSet() throws Exception {
        String type = randomBoolean() ? "text" : "long";
        String settings = "{\"index.number_of_shards\": 1, \"index.number_of_replicas\": 0}";
        SharedSignificantTermsTestMethods.index01Docs(type, settings, this);
        testBackgroundVsSeparateSet(new MutualInformation(true, true), new MutualInformation(true, false), type);
        testBackgroundVsSeparateSet(new ChiSquare(true, true), new ChiSquare(true, false), type);
        testBackgroundVsSeparateSet(new GND(true), new GND(false), type);
    }

    // compute significance score by
    // 1. terms agg on class and significant terms
    // 2. filter buckets and set the background to the other class and set is_background false
    // both should yield exact same result
    public void testBackgroundVsSeparateSet(
        SignificanceHeuristic significanceHeuristicExpectingSuperset,
        SignificanceHeuristic significanceHeuristicExpectingSeparateSets,
        String type
    ) throws Exception {

        final boolean useSigText = randomBoolean() && type.equals("text");
        SearchRequestBuilder request1;
        if (useSigText) {
            request1 = client().prepareSearch(INDEX_NAME)
                .addAggregation(
                    terms("class").field(CLASS_FIELD)
                        .subAggregation(
                            significantText("sig_terms", TEXT_FIELD).minDocCount(1)
                                .significanceHeuristic(significanceHeuristicExpectingSuperset)
                        )
                );
        } else {
            request1 = client().prepareSearch(INDEX_NAME)
                .addAggregation(
                    terms("class").field(CLASS_FIELD)
                        .subAggregation(
                            significantTerms("sig_terms").field(TEXT_FIELD)
                                .minDocCount(1)
                                .significanceHeuristic(significanceHeuristicExpectingSuperset)
                        )
                );
        }

        SearchResponse response1 = request1.get();
        assertSearchResponse(response1);

        SearchRequestBuilder request2;
        if (useSigText) {
            request2 = client().prepareSearch(INDEX_NAME)
                .addAggregation(
                    filter("0", QueryBuilders.termQuery(CLASS_FIELD, "0")).subAggregation(
                        significantText("sig_terms", TEXT_FIELD).minDocCount(1)
                            .backgroundFilter(QueryBuilders.termQuery(CLASS_FIELD, "1"))
                            .significanceHeuristic(significanceHeuristicExpectingSeparateSets)
                    )
                )
                .addAggregation(
                    filter("1", QueryBuilders.termQuery(CLASS_FIELD, "1")).subAggregation(
                        significantText("sig_terms", TEXT_FIELD).minDocCount(1)
                            .backgroundFilter(QueryBuilders.termQuery(CLASS_FIELD, "0"))
                            .significanceHeuristic(significanceHeuristicExpectingSeparateSets)
                    )
                );
        } else {
            request2 = client().prepareSearch(INDEX_NAME)
                .addAggregation(
                    filter("0", QueryBuilders.termQuery(CLASS_FIELD, "0")).subAggregation(
                        significantTerms("sig_terms").field(TEXT_FIELD)
                            .minDocCount(1)
                            .backgroundFilter(QueryBuilders.termQuery(CLASS_FIELD, "1"))
                            .significanceHeuristic(significanceHeuristicExpectingSeparateSets)
                    )
                )
                .addAggregation(
                    filter("1", QueryBuilders.termQuery(CLASS_FIELD, "1")).subAggregation(
                        significantTerms("sig_terms").field(TEXT_FIELD)
                            .minDocCount(1)
                            .backgroundFilter(QueryBuilders.termQuery(CLASS_FIELD, "0"))
                            .significanceHeuristic(significanceHeuristicExpectingSeparateSets)
                    )
                );
        }

        SearchResponse response2 = request2.get();

        StringTerms classes = response1.getAggregations().get("class");

        SignificantTerms sigTerms0 = ((SignificantTerms) (classes.getBucketByKey("0").getAggregations().asMap().get("sig_terms")));
        assertThat(sigTerms0.getBuckets().size(), equalTo(2));
        double score00Background = sigTerms0.getBucketByKey("0").getSignificanceScore();
        double score01Background = sigTerms0.getBucketByKey("1").getSignificanceScore();
        SignificantTerms sigTerms1 = ((SignificantTerms) (classes.getBucketByKey("1").getAggregations().asMap().get("sig_terms")));
        double score10Background = sigTerms1.getBucketByKey("0").getSignificanceScore();
        double score11Background = sigTerms1.getBucketByKey("1").getSignificanceScore();

        Aggregations aggs = response2.getAggregations();

        sigTerms0 = (SignificantTerms) ((InternalFilter) aggs.get("0")).getAggregations().getAsMap().get("sig_terms");
        double score00SeparateSets = sigTerms0.getBucketByKey("0").getSignificanceScore();
        double score01SeparateSets = sigTerms0.getBucketByKey("1").getSignificanceScore();

        sigTerms1 = (SignificantTerms) ((InternalFilter) aggs.get("1")).getAggregations().getAsMap().get("sig_terms");
        double score10SeparateSets = sigTerms1.getBucketByKey("0").getSignificanceScore();
        double score11SeparateSets = sigTerms1.getBucketByKey("1").getSignificanceScore();

        assertThat(score00Background, equalTo(score00SeparateSets));
        assertThat(score01Background, equalTo(score01SeparateSets));
        assertThat(score10Background, equalTo(score10SeparateSets));
        assertThat(score11Background, equalTo(score11SeparateSets));
    }

    public void testScoresEqualForPositiveAndNegative() throws Exception {
        indexEqualTestData();
        testScoresEqualForPositiveAndNegative(new MutualInformation(true, true));
        testScoresEqualForPositiveAndNegative(new ChiSquare(true, true));
    }

    public void testScoresEqualForPositiveAndNegative(SignificanceHeuristic heuristic) throws Exception {

        // check that results for both classes are the same with exclude negatives = false and classes are routing ids
        SearchRequestBuilder request;
        if (randomBoolean()) {
            request = client().prepareSearch("test")
                .addAggregation(
                    terms("class").field("class")
                        .subAggregation(
                            significantTerms("mySignificantTerms").field("text")
                                .executionHint(randomExecutionHint())
                                .significanceHeuristic(heuristic)
                                .minDocCount(1)
                                .shardSize(1000)
                                .size(1000)
                        )
                );
        } else {
            request = client().prepareSearch("test")
                .addAggregation(
                    terms("class").field("class")
                        .subAggregation(
                            significantText("mySignificantTerms", "text").significanceHeuristic(heuristic)
                                .minDocCount(1)
                                .shardSize(1000)
                                .size(1000)
                        )
                );
        }
        SearchResponse response = request.get();
        assertSearchResponse(response);

        assertSearchResponse(response);
        StringTerms classes = response.getAggregations().get("class");
        assertThat(classes.getBuckets().size(), equalTo(2));
        Iterator<? extends Terms.Bucket> classBuckets = classes.getBuckets().iterator();

        Aggregations aggregations = classBuckets.next().getAggregations();
        SignificantTerms sigTerms = aggregations.get("mySignificantTerms");

        List<? extends SignificantTerms.Bucket> classA = sigTerms.getBuckets();
        Iterator<SignificantTerms.Bucket> classBBucketIterator = sigTerms.iterator();
        assertThat(classA.size(), greaterThan(0));
        for (SignificantTerms.Bucket classABucket : classA) {
            SignificantTerms.Bucket classBBucket = classBBucketIterator.next();
            assertThat(classABucket.getKey(), equalTo(classBBucket.getKey()));
            assertThat(classABucket.getSignificanceScore(), closeTo(classBBucket.getSignificanceScore(), 1.e-5));
        }
    }

    /**
     * A simple test that adds a sub-aggregation to a significant terms aggregation,
     * to help check that sub-aggregation collection is handled correctly.
     */
    public void testSubAggregations() throws Exception {
        indexEqualTestData();

        QueryBuilder query = QueryBuilders.termsQuery(TEXT_FIELD, "a", "b");
        AggregationBuilder subAgg = terms("class").field(CLASS_FIELD);
        AggregationBuilder agg = significantTerms("significant_terms").field(TEXT_FIELD)
            .executionHint(randomExecutionHint())
            .significanceHeuristic(new ChiSquare(true, true))
            .minDocCount(1)
            .shardSize(1000)
            .size(1000)
            .subAggregation(subAgg);

        SearchResponse response = client().prepareSearch("test").setQuery(query).addAggregation(agg).get();
        assertSearchResponse(response);

        SignificantTerms sigTerms = response.getAggregations().get("significant_terms");
        assertThat(sigTerms.getBuckets().size(), equalTo(2));

        for (SignificantTerms.Bucket bucket : sigTerms) {
            StringTerms terms = bucket.getAggregations().get("class");
            assertThat(terms.getBuckets().size(), equalTo(2));
        }
    }

    private void indexEqualTestData() throws ExecutionException, InterruptedException {
        assertAcked(
            prepareCreate("test").setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0))
                .setMapping("text", "type=text,fielddata=true", "class", "type=keyword")
        );
        createIndex("idx_unmapped");

        ensureGreen();
        String data[] = {
            "A\ta",
            "A\ta",
            "A\tb",
            "A\tb",
            "A\tb",
            "B\tc",
            "B\tc",
            "B\tc",
            "B\tc",
            "B\td",
            "B\td",
            "B\td",
            "B\td",
            "B\td",
            "A\tc d",
            "B\ta b" };

        List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        for (int i = 0; i < data.length; i++) {
            String[] parts = data[i].split("\t");
            indexRequestBuilders.add(client().prepareIndex("test").setId("" + i).setSource("class", parts[0], "text", parts[1]));
        }
        indexRandom(true, false, indexRequestBuilders);
    }

    public void testScriptScore() throws ExecutionException, InterruptedException, IOException {
        String type = randomBoolean() ? "text" : "long";
        indexRandomFrequencies01(type);
        ScriptHeuristic scriptHeuristic = getScriptSignificanceHeuristic();

        SearchRequestBuilder request;
        if ("text".equals(type) && randomBoolean()) {
            request = client().prepareSearch(INDEX_NAME)
                .addAggregation(
                    terms("class").field(CLASS_FIELD)
                        .subAggregation(
                            significantText("mySignificantTerms", TEXT_FIELD).significanceHeuristic(scriptHeuristic)
                                .minDocCount(1)
                                .shardSize(2)
                                .size(2)
                        )
                );
        } else {
            request = client().prepareSearch(INDEX_NAME)
                .addAggregation(
                    terms("class").field(CLASS_FIELD)
                        .subAggregation(
                            significantTerms("mySignificantTerms").field(TEXT_FIELD)
                                .executionHint(randomExecutionHint())
                                .significanceHeuristic(scriptHeuristic)
                                .minDocCount(1)
                                .shardSize(2)
                                .size(2)
                        )
                );
        }
        SearchResponse response = request.get();
        assertSearchResponse(response);
        for (Terms.Bucket classBucket : ((Terms) response.getAggregations().get("class")).getBuckets()) {
            SignificantTerms sigTerms = classBucket.getAggregations().get("mySignificantTerms");
            for (SignificantTerms.Bucket bucket : sigTerms.getBuckets()) {
                assertThat(
                    bucket.getSignificanceScore(),
                    is((double) bucket.getSubsetDf() + bucket.getSubsetSize() + bucket.getSupersetDf() + bucket.getSupersetSize())
                );
            }
        }
    }

    private ScriptHeuristic getScriptSignificanceHeuristic() throws IOException {
        Script script;
        if (randomBoolean()) {
            Map<String, Object> params = new HashMap<>();
            params.put("param", randomIntBetween(1, 100));
            script = new Script(ScriptType.INLINE, "mockscript", "script_with_params", params);
        } else {
            script = new Script(ScriptType.INLINE, "mockscript", "script_no_params", Collections.emptyMap());
        }
        return new ScriptHeuristic(script);
    }

    private void indexRandomFrequencies01(String type) throws ExecutionException, InterruptedException {
        String textMappings = "type=" + type;
        if (type.equals("text")) {
            textMappings += ",fielddata=true";
        }
        assertAcked(prepareCreate(INDEX_NAME).setMapping(TEXT_FIELD, textMappings, CLASS_FIELD, "type=keyword"));
        String[] gb = { "0", "1" };
        List<IndexRequestBuilder> indexRequestBuilderList = new ArrayList<>();
        for (int i = 0; i < randomInt(20); i++) {
            int randNum = randomInt(2);
            String[] text = new String[1];
            if (randNum == 2) {
                text = gb;
            } else {
                text[0] = gb[randNum];
            }
            indexRequestBuilderList.add(
                client().prepareIndex(INDEX_NAME).setSource(TEXT_FIELD, text, CLASS_FIELD, randomBoolean() ? "one" : "zero")
            );
        }
        indexRandom(true, indexRequestBuilderList);
    }

    public void testReduceFromSeveralShards() throws IOException, ExecutionException, InterruptedException {
        SharedSignificantTermsTestMethods.aggregateAndCheckFromSeveralShards(this);
    }

    /**
     * Make sure that a request using a deterministic script or not using a script get cached.
     * Ensure requests using nondeterministic scripts do not get cached.
     */
    public void testScriptCaching() throws Exception {
        assertAcked(
            prepareCreate("cache_test_idx").setMapping("d", "type=long")
                .setSettings(Settings.builder().put("requests.cache.enable", true).put("number_of_shards", 1).put("number_of_replicas", 1))
                .get()
        );
        indexRandom(
            true,
            client().prepareIndex("cache_test_idx").setId("1").setSource("s", 1),
            client().prepareIndex("cache_test_idx").setId("2").setSource("s", 2)
        );

        // Make sure we are starting with a clear cache
        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getHitCount(),
            equalTo(0L)
        );
        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getMissCount(),
            equalTo(0L)
        );

        // Test that a request using a nondeterministic script does not get cached
        ScriptHeuristic scriptHeuristic = new ScriptHeuristic(
            new Script(ScriptType.INLINE, "mockscript", "Math.random()", Collections.emptyMap())
        );
        boolean useSigText = randomBoolean();
        SearchResponse r;
        if (useSigText) {
            r = client().prepareSearch("cache_test_idx")
                .setSize(0)
                .addAggregation(significantText("foo", "s").significanceHeuristic(scriptHeuristic))
                .get();
        } else {
            r = client().prepareSearch("cache_test_idx")
                .setSize(0)
                .addAggregation(significantTerms("foo").field("s").significanceHeuristic(scriptHeuristic))
                .get();
        }
        assertSearchResponse(r);

        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getHitCount(),
            equalTo(0L)
        );
        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getMissCount(),
            equalTo(0L)
        );

        // Test that a request using a deterministic script gets cached
        scriptHeuristic = getScriptSignificanceHeuristic();
        useSigText = randomBoolean();
        if (useSigText) {
            r = client().prepareSearch("cache_test_idx")
                .setSize(0)
                .addAggregation(significantText("foo", "s").significanceHeuristic(scriptHeuristic))
                .get();
        } else {
            r = client().prepareSearch("cache_test_idx")
                .setSize(0)
                .addAggregation(significantTerms("foo").field("s").significanceHeuristic(scriptHeuristic))
                .get();
        }
        assertSearchResponse(r);

        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getHitCount(),
            equalTo(0L)
        );
        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getMissCount(),
            equalTo(1L)
        );

        // Ensure that non-scripted requests are cached as normal
        if (useSigText) {
            r = client().prepareSearch("cache_test_idx").setSize(0).addAggregation(significantText("foo", "s")).get();
        } else {
            r = client().prepareSearch("cache_test_idx").setSize(0).addAggregation(significantTerms("foo").field("s")).get();
        }
        assertSearchResponse(r);

        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getHitCount(),
            equalTo(0L)
        );
        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getMissCount(),
            equalTo(2L)
        );
    }
}
