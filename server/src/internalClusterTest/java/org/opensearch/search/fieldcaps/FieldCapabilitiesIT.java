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

package org.opensearch.search.fieldcaps;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.fieldcaps.FieldCapabilities;
import org.opensearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.ParameterizedOpenSearchIntegTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

public class FieldCapabilitiesIT extends ParameterizedOpenSearchIntegTestCase {

    public FieldCapabilitiesIT(Settings dynamicSettings) {
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

    @Before
    public void setUp() throws Exception {
        super.setUp();

        XContentBuilder oldIndexMapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("distance")
            .field("type", "double")
            .endObject()
            .startObject("route_length_miles")
            .field("type", "alias")
            .field("path", "distance")
            .endObject()
            .startObject("playlist")
            .field("type", "text")
            .endObject()
            .startObject("secret_soundtrack")
            .field("type", "alias")
            .field("path", "playlist")
            .endObject()
            .startObject("old_field")
            .field("type", "long")
            .endObject()
            .startObject("new_field")
            .field("type", "alias")
            .field("path", "old_field")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        assertAcked(prepareCreate("old_index").setMapping(oldIndexMapping));

        XContentBuilder newIndexMapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("distance")
            .field("type", "text")
            .endObject()
            .startObject("route_length_miles")
            .field("type", "double")
            .endObject()
            .startObject("new_field")
            .field("type", "long")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        assertAcked(prepareCreate("new_index").setMapping(newIndexMapping));
        assertAcked(client().admin().indices().prepareAliases().addAlias("new_index", "current"));
    }

    public static class FieldFilterPlugin extends Plugin implements MapperPlugin {
        @Override
        public Function<String, Predicate<String>> getFieldFilter() {
            return index -> field -> !field.equals("playlist");
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(FieldFilterPlugin.class);
    }

    public void testFieldAlias() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("distance", "route_length_miles").get();

        assertIndices(response, "old_index", "new_index");
        // Ensure the response has entries for both requested fields.
        assertTrue(response.get().containsKey("distance"));
        assertTrue(response.get().containsKey("route_length_miles"));

        // Check the capabilities for the 'distance' field.
        Map<String, FieldCapabilities> distance = response.getField("distance");
        assertEquals(2, distance.size());

        assertTrue(distance.containsKey("double"));
        assertEquals(
            new FieldCapabilities("distance", "double", true, true, new String[] { "old_index" }, null, null, Collections.emptyMap()),
            distance.get("double")
        );

        assertTrue(distance.containsKey("text"));
        assertEquals(
            new FieldCapabilities("distance", "text", true, false, new String[] { "new_index" }, null, null, Collections.emptyMap()),
            distance.get("text")
        );

        // Check the capabilities for the 'route_length_miles' alias.
        Map<String, FieldCapabilities> routeLength = response.getField("route_length_miles");
        assertEquals(1, routeLength.size());

        assertTrue(routeLength.containsKey("double"));
        assertEquals(
            new FieldCapabilities("route_length_miles", "double", true, true, null, null, null, Collections.emptyMap()),
            routeLength.get("double")
        );
    }

    public void testFieldAliasWithWildcard() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("route*").get();

        assertIndices(response, "old_index", "new_index");
        assertEquals(1, response.get().size());
        assertTrue(response.get().containsKey("route_length_miles"));
    }

    public void testFieldAliasFiltering() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("secret-soundtrack", "route_length_miles").get();
        assertIndices(response, "old_index", "new_index");
        assertEquals(1, response.get().size());
        assertTrue(response.get().containsKey("route_length_miles"));
    }

    public void testFieldAliasFilteringWithWildcard() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("distance", "secret*").get();
        assertIndices(response, "old_index", "new_index");
        assertEquals(1, response.get().size());
        assertTrue(response.get().containsKey("distance"));
    }

    public void testWithUnmapped() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("new_field", "old_field").setIncludeUnmapped(true).get();
        assertIndices(response, "old_index", "new_index");

        assertEquals(2, response.get().size());
        assertTrue(response.get().containsKey("old_field"));

        Map<String, FieldCapabilities> oldField = response.getField("old_field");
        assertEquals(2, oldField.size());

        assertTrue(oldField.containsKey("long"));
        assertEquals(
            new FieldCapabilities("old_field", "long", true, true, new String[] { "old_index" }, null, null, Collections.emptyMap()),
            oldField.get("long")
        );

        assertTrue(oldField.containsKey("unmapped"));
        assertEquals(
            new FieldCapabilities("old_field", "unmapped", false, false, new String[] { "new_index" }, null, null, Collections.emptyMap()),
            oldField.get("unmapped")
        );

        Map<String, FieldCapabilities> newField = response.getField("new_field");
        assertEquals(1, newField.size());

        assertTrue(newField.containsKey("long"));
        assertEquals(
            new FieldCapabilities("new_field", "long", true, true, null, null, null, Collections.emptyMap()),
            newField.get("long")
        );
    }

    public void testWithIndexAlias() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps("current").setFields("*").get();
        assertIndices(response, "new_index");

        FieldCapabilitiesResponse response1 = client().prepareFieldCaps("current", "old_index").setFields("*").get();
        assertIndices(response1, "old_index", "new_index");
        FieldCapabilitiesResponse response2 = client().prepareFieldCaps("current", "old_index", "new_index").setFields("*").get();
        assertEquals(response1, response2);
    }

    public void testWithIndexFilter() throws InterruptedException {
        assumeFalse(
            "Concurrent search case muted pending fix: https://github.com/opensearch-project/OpenSearch/issues/10433",
            internalCluster().clusterService().getClusterSettings().get(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING)
        );
        assertAcked(prepareCreate("index-1").setMapping("timestamp", "type=date", "field1", "type=keyword"));
        assertAcked(prepareCreate("index-2").setMapping("timestamp", "type=date", "field1", "type=long"));

        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(client().prepareIndex("index-1").setSource("timestamp", "2015-07-08"));
        reqs.add(client().prepareIndex("index-1").setSource("timestamp", "2018-07-08"));
        reqs.add(client().prepareIndex("index-2").setSource("timestamp", "2019-10-12"));
        reqs.add(client().prepareIndex("index-2").setSource("timestamp", "2020-07-08"));
        indexRandom(true, reqs);

        FieldCapabilitiesResponse response = client().prepareFieldCaps("index-*").setFields("*").get();
        assertIndices(response, "index-1", "index-2");
        Map<String, FieldCapabilities> newField = response.getField("field1");
        assertEquals(2, newField.size());
        assertTrue(newField.containsKey("long"));
        assertTrue(newField.containsKey("keyword"));

        response = client().prepareFieldCaps("index-*")
            .setFields("*")
            .setIndexFilter(QueryBuilders.rangeQuery("timestamp").gte("2019-11-01"))
            .get();
        assertIndices(response, "index-2");
        newField = response.getField("field1");
        assertEquals(1, newField.size());
        assertTrue(newField.containsKey("long"));

        response = client().prepareFieldCaps("index-*")
            .setFields("*")
            .setIndexFilter(QueryBuilders.rangeQuery("timestamp").lte("2017-01-01"))
            .get();
        assertIndices(response, "index-1");
        newField = response.getField("field1");
        assertEquals(1, newField.size());
        assertTrue(newField.containsKey("keyword"));
    }

    private void assertIndices(FieldCapabilitiesResponse response, String... indices) {
        assertNotNull(response.getIndices());
        Arrays.sort(indices);
        Arrays.sort(response.getIndices());
        assertArrayEquals(indices, response.getIndices());
    }
}
