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

package org.opensearch.indices;

import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchHits;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class DateMathIndexExpressionsIntegrationIT extends OpenSearchIntegTestCase {

    public void testIndexNameDateMathExpressions() {
        DateTime now = new DateTime(DateTimeZone.UTC);
        String index1 = ".marvel-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(now);
        String index2 = ".marvel-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(now.minusDays(1));
        String index3 = ".marvel-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(now.minusDays(2));
        createIndex(index1, index2, index3);

        GetSettingsResponse getSettingsResponse = client().admin().indices().prepareGetSettings(index1, index2, index3).get();
        assertEquals(index1, getSettingsResponse.getSetting(index1, IndexMetadata.SETTING_INDEX_PROVIDED_NAME));
        assertEquals(index2, getSettingsResponse.getSetting(index2, IndexMetadata.SETTING_INDEX_PROVIDED_NAME));
        assertEquals(index3, getSettingsResponse.getSetting(index3, IndexMetadata.SETTING_INDEX_PROVIDED_NAME));

        String dateMathExp1 = "<.marvel-{now/d}>";
        String dateMathExp2 = "<.marvel-{now/d-1d}>";
        String dateMathExp3 = "<.marvel-{now/d-2d}>";
        client().prepareIndex(dateMathExp1).setId("1").setSource("{}", MediaTypeRegistry.JSON).get();
        client().prepareIndex(dateMathExp2).setId("2").setSource("{}", MediaTypeRegistry.JSON).get();
        client().prepareIndex(dateMathExp3).setId("3").setSource("{}", MediaTypeRegistry.JSON).get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch(dateMathExp1, dateMathExp2, dateMathExp3).get();
        assertHitCount(searchResponse, 3);
        assertSearchHits(searchResponse, "1", "2", "3");

        GetResponse getResponse = client().prepareGet(dateMathExp1, "1").get();
        assertThat(getResponse.isExists(), is(true));
        assertThat(getResponse.getId(), equalTo("1"));

        getResponse = client().prepareGet(dateMathExp2, "2").get();
        assertThat(getResponse.isExists(), is(true));
        assertThat(getResponse.getId(), equalTo("2"));

        getResponse = client().prepareGet(dateMathExp3, "3").get();
        assertThat(getResponse.isExists(), is(true));
        assertThat(getResponse.getId(), equalTo("3"));

        MultiGetResponse mgetResponse = client().prepareMultiGet()
            .add(dateMathExp1, "1")
            .add(dateMathExp2, "2")
            .add(dateMathExp3, "3")
            .get();
        assertThat(mgetResponse.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(mgetResponse.getResponses()[0].getResponse().getId(), equalTo("1"));
        assertThat(mgetResponse.getResponses()[1].getResponse().isExists(), is(true));
        assertThat(mgetResponse.getResponses()[1].getResponse().getId(), equalTo("2"));
        assertThat(mgetResponse.getResponses()[2].getResponse().isExists(), is(true));
        assertThat(mgetResponse.getResponses()[2].getResponse().getId(), equalTo("3"));

        IndicesStatsResponse indicesStatsResponse = client().admin().indices().prepareStats(dateMathExp1, dateMathExp2, dateMathExp3).get();
        assertThat(indicesStatsResponse.getIndex(index1), notNullValue());
        assertThat(indicesStatsResponse.getIndex(index2), notNullValue());
        assertThat(indicesStatsResponse.getIndex(index3), notNullValue());

        DeleteResponse deleteResponse = client().prepareDelete(dateMathExp1, "1").get();
        assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());
        assertThat(deleteResponse.getId(), equalTo("1"));

        deleteResponse = client().prepareDelete(dateMathExp2, "2").get();
        assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());
        assertThat(deleteResponse.getId(), equalTo("2"));

        deleteResponse = client().prepareDelete(dateMathExp3, "3").get();
        assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());
        assertThat(deleteResponse.getId(), equalTo("3"));
    }

    public void testAutoCreateIndexWithDateMathExpression() throws Exception {
        DateTime now = new DateTime(DateTimeZone.UTC);
        String index1 = ".marvel-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(now);
        String index2 = ".marvel-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(now.minusDays(1));
        String index3 = ".marvel-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(now.minusDays(2));

        String dateMathExp1 = "<.marvel-{now/d}>";
        String dateMathExp2 = "<.marvel-{now/d-1d}>";
        String dateMathExp3 = "<.marvel-{now/d-2d}>";
        client().prepareIndex(dateMathExp1).setId("1").setSource("{}", MediaTypeRegistry.JSON).get();
        client().prepareIndex(dateMathExp2).setId("2").setSource("{}", MediaTypeRegistry.JSON).get();
        client().prepareIndex(dateMathExp3).setId("3").setSource("{}", MediaTypeRegistry.JSON).get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch(dateMathExp1, dateMathExp2, dateMathExp3).get();
        assertHitCount(searchResponse, 3);
        assertSearchHits(searchResponse, "1", "2", "3");

        IndicesStatsResponse indicesStatsResponse = client().admin().indices().prepareStats(dateMathExp1, dateMathExp2, dateMathExp3).get();
        assertThat(indicesStatsResponse.getIndex(index1), notNullValue());
        assertThat(indicesStatsResponse.getIndex(index2), notNullValue());
        assertThat(indicesStatsResponse.getIndex(index3), notNullValue());
    }

    public void testCreateIndexWithDateMathExpression() throws Exception {
        DateTime now = new DateTime(DateTimeZone.UTC);
        String index1 = ".marvel-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(now);
        String index2 = ".marvel-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(now.minusDays(1));
        String index3 = ".marvel-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(now.minusDays(2));

        String dateMathExp1 = "<.marvel-{now/d}>";
        String dateMathExp2 = "<.marvel-{now/d-1d}>";
        String dateMathExp3 = "<.marvel-{now/d-2d}>";
        createIndex(dateMathExp1, dateMathExp2, dateMathExp3);

        GetSettingsResponse getSettingsResponse = client().admin().indices().prepareGetSettings(index1, index2, index3).get();
        assertEquals(dateMathExp1, getSettingsResponse.getSetting(index1, IndexMetadata.SETTING_INDEX_PROVIDED_NAME));
        assertEquals(dateMathExp2, getSettingsResponse.getSetting(index2, IndexMetadata.SETTING_INDEX_PROVIDED_NAME));
        assertEquals(dateMathExp3, getSettingsResponse.getSetting(index3, IndexMetadata.SETTING_INDEX_PROVIDED_NAME));

        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        assertThat(clusterState.metadata().index(index1), notNullValue());
        assertThat(clusterState.metadata().index(index2), notNullValue());
        assertThat(clusterState.metadata().index(index3), notNullValue());
    }

}
