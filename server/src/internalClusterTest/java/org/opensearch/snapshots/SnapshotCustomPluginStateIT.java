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

package org.opensearch.snapshots;

import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.opensearch.action.admin.cluster.storedscripts.GetStoredScriptResponse;
import org.opensearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.opensearch.action.ingest.DeletePipelineRequest;
import org.opensearch.action.ingest.GetPipelineResponse;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.ingest.IngestTestPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.MockScriptEngine;
import org.opensearch.script.StoredScriptsIT;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertIndexTemplateExists;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertIndexTemplateMissing;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class SnapshotCustomPluginStateIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(IngestTestPlugin.class, StoredScriptsIT.CustomScriptPlugin.class);
    }

    public void testIncludeGlobalState() throws Exception {
        createRepository("test-repo", "fs");

        boolean testTemplate = randomBoolean();
        boolean testPipeline = randomBoolean();
        boolean testScript = (testTemplate == false && testPipeline == false) || randomBoolean(); // At least something should be stored

        if (testTemplate) {
            logger.info("-->  creating test template");
            assertThat(
                client().admin()
                    .indices()
                    .preparePutTemplate("test-template")
                    .setPatterns(Collections.singletonList("te*"))
                    .setMapping(
                        XContentFactory.jsonBuilder()
                            .startObject()
                            .startObject("properties")
                            .startObject("field1")
                            .field("type", "text")
                            .field("store", true)
                            .endObject()
                            .startObject("field2")
                            .field("type", "keyword")
                            .field("store", true)
                            .endObject()
                            .endObject()
                            .endObject()
                    )
                    .get()
                    .isAcknowledged(),
                equalTo(true)
            );
        }

        if (testPipeline) {
            logger.info("-->  creating test pipeline");
            BytesReference pipelineSource = BytesReference.bytes(
                jsonBuilder().startObject()
                    .field("description", "my_pipeline")
                    .startArray("processors")
                    .startObject()
                    .startObject("test")
                    .endObject()
                    .endObject()
                    .endArray()
                    .endObject()
            );
            assertAcked(clusterAdmin().preparePutPipeline("barbaz", pipelineSource, MediaTypeRegistry.JSON).get());
        }

        if (testScript) {
            logger.info("-->  creating test script");
            assertAcked(
                clusterAdmin().preparePutStoredScript()
                    .setId("foobar")
                    .setContent(
                        new BytesArray("{\"script\": { \"lang\": \"" + MockScriptEngine.NAME + "\", \"source\": \"1\"} }"),
                        MediaTypeRegistry.JSON
                    )
            );
        }

        logger.info("--> snapshot without global state");
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap-no-global-state")
            .setIndices()
            .setIncludeGlobalState(false)
            .setWaitForCompletion(true)
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), equalTo(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(0));
        assertThat(getSnapshot("test-repo", "test-snap-no-global-state").state(), equalTo(SnapshotState.SUCCESS));
        SnapshotsStatusResponse snapshotsStatusResponse = clusterAdmin().prepareSnapshotStatus("test-repo")
            .addSnapshots("test-snap-no-global-state")
            .get();
        assertThat(snapshotsStatusResponse.getSnapshots().size(), equalTo(1));
        SnapshotStatus snapshotStatus = snapshotsStatusResponse.getSnapshots().get(0);
        assertThat(snapshotStatus.includeGlobalState(), equalTo(false));

        logger.info("--> snapshot with global state");
        createSnapshotResponse = clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap-with-global-state")
            .setIndices()
            .setIncludeGlobalState(true)
            .setWaitForCompletion(true)
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), equalTo(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(0));
        assertThat(getSnapshot("test-repo", "test-snap-with-global-state").state(), equalTo(SnapshotState.SUCCESS));
        snapshotsStatusResponse = clusterAdmin().prepareSnapshotStatus("test-repo").addSnapshots("test-snap-with-global-state").get();
        assertThat(snapshotsStatusResponse.getSnapshots().size(), equalTo(1));
        snapshotStatus = snapshotsStatusResponse.getSnapshots().get(0);
        assertThat(snapshotStatus.includeGlobalState(), equalTo(true));

        if (testTemplate) {
            logger.info("-->  delete test template");
            cluster().wipeTemplates("test-template");
            GetIndexTemplatesResponse getIndexTemplatesResponse = client().admin().indices().prepareGetTemplates().get();
            assertIndexTemplateMissing(getIndexTemplatesResponse, "test-template");
        }

        if (testPipeline) {
            logger.info("-->  delete test pipeline");
            assertAcked(clusterAdmin().deletePipeline(new DeletePipelineRequest("barbaz")).get());
        }

        if (testScript) {
            logger.info("-->  delete test script");
            assertAcked(clusterAdmin().prepareDeleteStoredScript("foobar").get());
        }

        logger.info("--> try restoring cluster state from snapshot without global state");
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap-no-global-state")
            .setWaitForCompletion(true)
            .setRestoreGlobalState(true)
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), equalTo(0));

        logger.info("--> check that template wasn't restored");
        GetIndexTemplatesResponse getIndexTemplatesResponse = client().admin().indices().prepareGetTemplates().get();
        assertIndexTemplateMissing(getIndexTemplatesResponse, "test-template");

        logger.info("--> restore cluster state");
        restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap-with-global-state")
            .setWaitForCompletion(true)
            .setRestoreGlobalState(true)
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), equalTo(0));

        if (testTemplate) {
            logger.info("--> check that template is restored");
            getIndexTemplatesResponse = client().admin().indices().prepareGetTemplates().get();
            assertIndexTemplateExists(getIndexTemplatesResponse, "test-template");
        }

        if (testPipeline) {
            logger.info("--> check that pipeline is restored");
            GetPipelineResponse getPipelineResponse = clusterAdmin().prepareGetPipeline("barbaz").get();
            assertTrue(getPipelineResponse.isFound());
        }

        if (testScript) {
            logger.info("--> check that script is restored");
            GetStoredScriptResponse getStoredScriptResponse = clusterAdmin().prepareGetStoredScript("foobar").get();
            assertNotNull(getStoredScriptResponse.getSource());
        }

        createIndexWithRandomDocs("test-idx", 100);

        logger.info("--> snapshot without global state but with indices");
        createSnapshotResponse = clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap-no-global-state-with-index")
            .setIndices("test-idx")
            .setIncludeGlobalState(false)
            .setWaitForCompletion(true)
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );
        assertThat(getSnapshot("test-repo", "test-snap-no-global-state-with-index").state(), equalTo(SnapshotState.SUCCESS));

        logger.info("-->  delete global state and index ");
        cluster().wipeIndices("test-idx");
        if (testTemplate) {
            cluster().wipeTemplates("test-template");
        }
        if (testPipeline) {
            assertAcked(clusterAdmin().deletePipeline(new DeletePipelineRequest("barbaz")).get());
        }

        if (testScript) {
            assertAcked(clusterAdmin().prepareDeleteStoredScript("foobar").get());
        }

        getIndexTemplatesResponse = client().admin().indices().prepareGetTemplates().get();
        assertIndexTemplateMissing(getIndexTemplatesResponse, "test-template");

        logger.info("--> try restoring index and cluster state from snapshot without global state");
        restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap-no-global-state-with-index")
            .setWaitForCompletion(true)
            .setRestoreGlobalState(true)
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));

        logger.info("--> check that global state wasn't restored but index was");
        getIndexTemplatesResponse = client().admin().indices().prepareGetTemplates().get();
        assertIndexTemplateMissing(getIndexTemplatesResponse, "test-template");
        assertFalse(clusterAdmin().prepareGetPipeline("barbaz").get().isFound());
        assertNull(clusterAdmin().prepareGetStoredScript("foobar").get().getSource());
        assertDocCount("test-idx", 100L);
    }
}
