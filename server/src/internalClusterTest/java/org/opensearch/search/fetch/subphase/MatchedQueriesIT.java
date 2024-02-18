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

package org.opensearch.search.fetch.subphase;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentHelper;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.test.ParameterizedOpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

import static org.opensearch.index.query.QueryBuilders.boolQuery;
import static org.opensearch.index.query.QueryBuilders.constantScoreQuery;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.matchQuery;
import static org.opensearch.index.query.QueryBuilders.queryStringQuery;
import static org.opensearch.index.query.QueryBuilders.rangeQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.index.query.QueryBuilders.termsQuery;
import static org.opensearch.index.query.QueryBuilders.wrapperQuery;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItemInArray;

public class MatchedQueriesIT extends ParameterizedOpenSearchIntegTestCase {

    public MatchedQueriesIT(Settings dynamicSettings) {
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

    public void testSimpleMatchedQueryFromFilteredQuery() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource("name", "test1", "number", 1).get();
        client().prepareIndex("test").setId("2").setSource("name", "test2", "number", 2).get();
        client().prepareIndex("test").setId("3").setSource("name", "test3", "number", 3).get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(
                boolQuery().must(matchAllQuery())
                    .filter(
                        boolQuery().should(rangeQuery("number").lt(2).queryName("test1"))
                            .should(rangeQuery("number").gte(2).queryName("test2"))
                    )
            )
            .get();
        assertHitCount(searchResponse, 3L);
        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.getId().equals("3") || hit.getId().equals("2")) {
                assertThat(hit.getMatchedQueries().length, equalTo(1));
                assertThat(hit.getMatchedQueries(), hasItemInArray("test2"));
            } else if (hit.getId().equals("1")) {
                assertThat(hit.getMatchedQueries().length, equalTo(1));
                assertThat(hit.getMatchedQueries(), hasItemInArray("test1"));
            } else {
                fail("Unexpected document returned with id " + hit.getId());
            }
        }

        searchResponse = client().prepareSearch()
            .setQuery(
                boolQuery().should(rangeQuery("number").lte(2).queryName("test1")).should(rangeQuery("number").gt(2).queryName("test2"))
            )
            .get();
        assertHitCount(searchResponse, 3L);
        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.getId().equals("1") || hit.getId().equals("2")) {
                assertThat(hit.getMatchedQueries().length, equalTo(1));
                assertThat(hit.getMatchedQueries(), hasItemInArray("test1"));
            } else if (hit.getId().equals("3")) {
                assertThat(hit.getMatchedQueries().length, equalTo(1));
                assertThat(hit.getMatchedQueries(), hasItemInArray("test2"));
            } else {
                fail("Unexpected document returned with id " + hit.getId());
            }
        }
    }

    public void testSimpleMatchedQueryFromTopLevelFilter() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource("name", "test", "title", "title1").get();
        client().prepareIndex("test").setId("2").setSource("name", "test").get();
        client().prepareIndex("test").setId("3").setSource("name", "test").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .setPostFilter(
                boolQuery().should(termQuery("name", "test").queryName("name")).should(termQuery("title", "title1").queryName("title"))
            )
            .get();
        assertHitCount(searchResponse, 3L);
        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.getId().equals("1")) {
                assertThat(hit.getMatchedQueries().length, equalTo(2));
                assertThat(hit.getMatchedQueries(), hasItemInArray("name"));
                assertThat(hit.getMatchedQueries(), hasItemInArray("title"));
            } else if (hit.getId().equals("2") || hit.getId().equals("3")) {
                assertThat(hit.getMatchedQueries().length, equalTo(1));
                assertThat(hit.getMatchedQueries(), hasItemInArray("name"));
            } else {
                fail("Unexpected document returned with id " + hit.getId());
            }
        }

        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .setPostFilter(
                boolQuery().should(termQuery("name", "test").queryName("name")).should(termQuery("title", "title1").queryName("title"))
            )
            .get();

        assertHitCount(searchResponse, 3L);
        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.getId().equals("1")) {
                assertThat(hit.getMatchedQueries().length, equalTo(2));
                assertThat(hit.getMatchedQueries(), hasItemInArray("name"));
                assertThat(hit.getMatchedQueries(), hasItemInArray("title"));
            } else if (hit.getId().equals("2") || hit.getId().equals("3")) {
                assertThat(hit.getMatchedQueries().length, equalTo(1));
                assertThat(hit.getMatchedQueries(), hasItemInArray("name"));
            } else {
                fail("Unexpected document returned with id " + hit.getId());
            }
        }
    }

    public void testSimpleMatchedQueryFromTopLevelFilterAndFilteredQuery() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource("name", "test", "title", "title1").get();
        client().prepareIndex("test").setId("2").setSource("name", "test", "title", "title2").get();
        client().prepareIndex("test").setId("3").setSource("name", "test", "title", "title3").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(boolQuery().must(matchAllQuery()).filter(termsQuery("title", "title1", "title2", "title3").queryName("title")))
            .setPostFilter(termQuery("name", "test").queryName("name"))
            .get();
        assertHitCount(searchResponse, 3L);
        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.getId().equals("1") || hit.getId().equals("2") || hit.getId().equals("3")) {
                assertThat(hit.getMatchedQueries().length, equalTo(2));
                assertThat(hit.getMatchedQueries(), hasItemInArray("name"));
                assertThat(hit.getMatchedQueries(), hasItemInArray("title"));
            } else {
                fail("Unexpected document returned with id " + hit.getId());
            }
        }

        searchResponse = client().prepareSearch()
            .setQuery(termsQuery("title", "title1", "title2", "title3").queryName("title"))
            .setPostFilter(matchQuery("name", "test").queryName("name"))
            .get();
        assertHitCount(searchResponse, 3L);
        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.getId().equals("1") || hit.getId().equals("2") || hit.getId().equals("3")) {
                assertThat(hit.getMatchedQueries().length, equalTo(2));
                assertThat(hit.getMatchedQueries(), hasItemInArray("name"));
                assertThat(hit.getMatchedQueries(), hasItemInArray("title"));
            } else {
                fail("Unexpected document returned with id " + hit.getId());
            }
        }
    }

    public void testRegExpQuerySupportsName() {
        createIndex("test1");
        ensureGreen();

        client().prepareIndex("test1").setId("1").setSource("title", "title1").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(QueryBuilders.regexpQuery("title", "title1").queryName("regex"))
            .get();
        assertHitCount(searchResponse, 1L);

        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.getId().equals("1")) {
                assertThat(hit.getMatchedQueries().length, equalTo(1));
                assertThat(hit.getMatchedQueries(), hasItemInArray("regex"));
            } else {
                fail("Unexpected document returned with id " + hit.getId());
            }
        }
    }

    public void testPrefixQuerySupportsName() {
        createIndex("test1");
        ensureGreen();

        client().prepareIndex("test1").setId("1").setSource("title", "title1").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(QueryBuilders.prefixQuery("title", "title").queryName("prefix"))
            .get();
        assertHitCount(searchResponse, 1L);

        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.getId().equals("1")) {
                assertThat(hit.getMatchedQueries().length, equalTo(1));
                assertThat(hit.getMatchedQueries(), hasItemInArray("prefix"));
            } else {
                fail("Unexpected document returned with id " + hit.getId());
            }
        }
    }

    public void testFuzzyQuerySupportsName() {
        createIndex("test1");
        ensureGreen();

        client().prepareIndex("test1").setId("1").setSource("title", "title1").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(QueryBuilders.fuzzyQuery("title", "titel1").queryName("fuzzy"))
            .get();
        assertHitCount(searchResponse, 1L);

        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.getId().equals("1")) {
                assertThat(hit.getMatchedQueries().length, equalTo(1));
                assertThat(hit.getMatchedQueries(), hasItemInArray("fuzzy"));
            } else {
                fail("Unexpected document returned with id " + hit.getId());
            }
        }
    }

    public void testWildcardQuerySupportsName() {
        createIndex("test1");
        ensureGreen();

        client().prepareIndex("test1").setId("1").setSource("title", "title1").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(QueryBuilders.wildcardQuery("title", "titl*").queryName("wildcard"))
            .get();
        assertHitCount(searchResponse, 1L);

        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.getId().equals("1")) {
                assertThat(hit.getMatchedQueries().length, equalTo(1));
                assertThat(hit.getMatchedQueries(), hasItemInArray("wildcard"));
            } else {
                fail("Unexpected document returned with id " + hit.getId());
            }
        }
    }

    public void testSpanFirstQuerySupportsName() {
        createIndex("test1");
        ensureGreen();

        client().prepareIndex("test1").setId("1").setSource("title", "title1 title2").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(QueryBuilders.spanFirstQuery(QueryBuilders.spanTermQuery("title", "title1"), 10).queryName("span"))
            .get();
        assertHitCount(searchResponse, 1L);

        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.getId().equals("1")) {
                assertThat(hit.getMatchedQueries().length, equalTo(1));
                assertThat(hit.getMatchedQueries(), hasItemInArray("span"));
            } else {
                fail("Unexpected document returned with id " + hit.getId());
            }
        }
    }

    /**
     * Test case for issue #4361: https://github.com/elastic/elasticsearch/issues/4361
     */
    public void testMatchedWithShould() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource("content", "Lorem ipsum dolor sit amet").get();
        client().prepareIndex("test").setId("2").setSource("content", "consectetur adipisicing elit").get();
        refresh();

        // Execute search at least two times to load it in cache
        int iter = scaledRandomIntBetween(2, 10);
        for (int i = 0; i < iter; i++) {
            SearchResponse searchResponse = client().prepareSearch()
                .setQuery(
                    boolQuery().minimumShouldMatch(1)
                        .should(queryStringQuery("dolor").queryName("dolor"))
                        .should(queryStringQuery("elit").queryName("elit"))
                )
                .setPreference("_primary")
                .get();

            assertHitCount(searchResponse, 2L);
            for (SearchHit hit : searchResponse.getHits()) {
                if (hit.getId().equals("1")) {
                    assertThat(hit.getMatchedQueries().length, equalTo(1));
                    assertThat(hit.getMatchedQueries(), hasItemInArray("dolor"));
                } else if (hit.getId().equals("2")) {
                    assertThat(hit.getMatchedQueries().length, equalTo(1));
                    assertThat(hit.getMatchedQueries(), hasItemInArray("elit"));
                } else {
                    fail("Unexpected document returned with id " + hit.getId());
                }
            }
        }
    }

    public void testMatchedWithWrapperQuery() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource("content", "Lorem ipsum dolor sit amet").get();
        refresh();

        MatchQueryBuilder matchQueryBuilder = matchQuery("content", "amet").queryName("abc");
        BytesReference matchBytes = XContentHelper.toXContent(matchQueryBuilder, MediaTypeRegistry.JSON, false);
        TermQueryBuilder termQueryBuilder = termQuery("content", "amet").queryName("abc");
        BytesReference termBytes = XContentHelper.toXContent(termQueryBuilder, MediaTypeRegistry.JSON, false);
        QueryBuilder[] queries = new QueryBuilder[] { wrapperQuery(matchBytes), constantScoreQuery(wrapperQuery(termBytes)) };
        for (QueryBuilder query : queries) {
            SearchResponse searchResponse = client().prepareSearch().setQuery(query).get();
            assertHitCount(searchResponse, 1L);
            assertThat(searchResponse.getHits().getAt(0).getMatchedQueries()[0], equalTo("abc"));
        }
    }
}
