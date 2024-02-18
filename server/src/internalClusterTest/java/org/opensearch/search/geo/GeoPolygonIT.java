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

package org.opensearch.search.geo;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.Version;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.search.SearchHit;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedOpenSearchIntegTestCase;
import org.opensearch.test.VersionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.boolQuery;
import static org.opensearch.index.query.QueryBuilders.geoPolygonQuery;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

@OpenSearchIntegTestCase.SuiteScopeTestCase
public class GeoPolygonIT extends ParameterizedOpenSearchIntegTestCase {

    public GeoPolygonIT(Settings dynamicSettings) {
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
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();

        assertAcked(
            prepareCreate("test").setSettings(settings).setMapping("location", "type=geo_point", "alias", "type=alias,path=location")
        );
        ensureGreen();

        indexRandom(
            true,
            client().prepareIndex("test")
                .setId("1")
                .setSource(
                    jsonBuilder().startObject()
                        .field("name", "New York")
                        .startObject("location")
                        .field("lat", 40.714)
                        .field("lon", -74.006)
                        .endObject()
                        .endObject()
                ),
            // to NY: 5.286 km
            client().prepareIndex("test")
                .setId("2")
                .setSource(
                    jsonBuilder().startObject()
                        .field("name", "Times Square")
                        .startObject("location")
                        .field("lat", 40.759)
                        .field("lon", -73.984)
                        .endObject()
                        .endObject()
                ),
            // to NY: 0.4621 km
            client().prepareIndex("test")
                .setId("3")
                .setSource(
                    jsonBuilder().startObject()
                        .field("name", "Tribeca")
                        .startObject("location")
                        .field("lat", 40.718)
                        .field("lon", -74.008)
                        .endObject()
                        .endObject()
                ),
            // to NY: 1.055 km
            client().prepareIndex("test")
                .setId("4")
                .setSource(
                    jsonBuilder().startObject()
                        .field("name", "Wall Street")
                        .startObject("location")
                        .field("lat", 40.705)
                        .field("lon", -74.009)
                        .endObject()
                        .endObject()
                ),
            // to NY: 1.258 km
            client().prepareIndex("test")
                .setId("5")
                .setSource(
                    jsonBuilder().startObject()
                        .field("name", "Soho")
                        .startObject("location")
                        .field("lat", 40.725)
                        .field("lon", -74)
                        .endObject()
                        .endObject()
                ),
            // to NY: 2.029 km
            client().prepareIndex("test")
                .setId("6")
                .setSource(
                    jsonBuilder().startObject()
                        .field("name", "Greenwich Village")
                        .startObject("location")
                        .field("lat", 40.731)
                        .field("lon", -73.996)
                        .endObject()
                        .endObject()
                ),
            // to NY: 8.572 km
            client().prepareIndex("test")
                .setId("7")
                .setSource(
                    jsonBuilder().startObject()
                        .field("name", "Brooklyn")
                        .startObject("location")
                        .field("lat", 40.65)
                        .field("lon", -73.95)
                        .endObject()
                        .endObject()
                )
        );
        ensureSearchable("test");
    }

    public void testSimplePolygon() throws Exception {
        List<GeoPoint> points = new ArrayList<>();
        points.add(new GeoPoint(40.7, -74.0));
        points.add(new GeoPoint(40.7, -74.1));
        points.add(new GeoPoint(40.8, -74.1));
        points.add(new GeoPoint(40.8, -74.0));
        points.add(new GeoPoint(40.7, -74.0));
        SearchResponse searchResponse = client().prepareSearch("test") // from NY
            .setQuery(boolQuery().must(geoPolygonQuery("location", points)))
            .get();
        assertHitCount(searchResponse, 4);
        assertThat(searchResponse.getHits().getHits().length, equalTo(4));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getId(), anyOf(equalTo("1"), equalTo("3"), equalTo("4"), equalTo("5")));
        }
    }

    public void testSimpleUnclosedPolygon() throws Exception {
        List<GeoPoint> points = new ArrayList<>();
        points.add(new GeoPoint(40.7, -74.0));
        points.add(new GeoPoint(40.7, -74.1));
        points.add(new GeoPoint(40.8, -74.1));
        points.add(new GeoPoint(40.8, -74.0));
        SearchResponse searchResponse = client().prepareSearch("test") // from NY
            .setQuery(boolQuery().must(geoPolygonQuery("location", points)))
            .get();
        assertHitCount(searchResponse, 4);
        assertThat(searchResponse.getHits().getHits().length, equalTo(4));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getId(), anyOf(equalTo("1"), equalTo("3"), equalTo("4"), equalTo("5")));
        }
    }

    public void testFieldAlias() {
        List<GeoPoint> points = new ArrayList<>();
        points.add(new GeoPoint(40.7, -74.0));
        points.add(new GeoPoint(40.7, -74.1));
        points.add(new GeoPoint(40.8, -74.1));
        points.add(new GeoPoint(40.8, -74.0));
        points.add(new GeoPoint(40.7, -74.0));
        SearchResponse searchResponse = client().prepareSearch("test") // from NY
            .setQuery(boolQuery().must(geoPolygonQuery("alias", points)))
            .get();
        assertHitCount(searchResponse, 4);
    }
}
