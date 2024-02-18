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

package org.opensearch.ingest.common;

import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.ingest.IngestStats;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.MockScriptEngine;
import org.opensearch.script.MockScriptPlugin;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest.Metric.INGEST;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

// Ideally I like this test to live in the server module, but otherwise a large part of the ScriptProcessor
// ends up being copied into this test.
@OpenSearchIntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0, scope = OpenSearchIntegTestCase.Scope.TEST)
public class IngestRestartIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(IngestCommonModulePlugin.class, CustomScriptPlugin.class);
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {
        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> pluginScripts = new HashMap<>();
            pluginScripts.put("my_script", ctx -> {
                ctx.put("z", 0);
                return null;
            });
            pluginScripts.put("throwing_script", ctx -> { throw new RuntimeException("this script always fails"); });
            return pluginScripts;
        }
    }

    public void testFailureInConditionalProcessor() {
        internalCluster().ensureAtLeastNumDataNodes(1);
        internalCluster().startClusterManagerOnlyNode();
        final String pipelineId = "foo";
        client().admin()
            .cluster()
            .preparePutPipeline(
                pipelineId,
                new BytesArray(
                    "{\n"
                        + "  \"processors\" : [\n"
                        + "  {\"set\" : {\"field\": \"any_field\", \"value\": \"any_value\"}},\n"
                        + "  {\"set\" : {"
                        + ""
                        + "    \"if\" : "
                        + "{\"lang\": \""
                        + MockScriptEngine.NAME
                        + "\", \"source\": \"throwing_script\"},"
                        + "    \"field\": \"any_field2\","
                        + "    \"value\": \"any_value2\"}"
                        + "  }\n"
                        + "  ]\n"
                        + "}"
                ),
                MediaTypeRegistry.JSON
            )
            .get();

        Exception e = expectThrows(
            Exception.class,
            () -> client().prepareIndex("index")
                .setId("1")
                .setSource("x", 0)
                .setPipeline(pipelineId)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get()
        );
        assertTrue(e.getMessage().contains("this script always fails"));

        NodesStatsResponse r = client().admin()
            .cluster()
            .prepareNodesStats(internalCluster().getNodeNames())
            .addMetric(INGEST.metricName())
            .get();
        int nodeCount = r.getNodes().size();
        for (int k = 0; k < nodeCount; k++) {
            List<IngestStats.ProcessorStat> stats = r.getNodes().get(k).getIngestStats().getProcessorStats().get(pipelineId);
            for (IngestStats.ProcessorStat st : stats) {
                assertThat(st.getStats().getCurrent(), greaterThanOrEqualTo(0L));
            }
        }
    }

    public void testScriptDisabled() throws Exception {
        String pipelineIdWithoutScript = randomAlphaOfLengthBetween(5, 10);
        String pipelineIdWithScript = pipelineIdWithoutScript + "_script";
        internalCluster().startNode();

        BytesReference pipelineWithScript = new BytesArray(
            "{\n"
                + "  \"processors\" : [\n"
                + "      {\"script\" : {\"lang\": \""
                + MockScriptEngine.NAME
                + "\", \"source\": \"my_script\"}}\n"
                + "  ]\n"
                + "}"
        );
        BytesReference pipelineWithoutScript = new BytesArray(
            "{\n" + "  \"processors\" : [\n" + "      {\"set\" : {\"field\": \"y\", \"value\": 0}}\n" + "  ]\n" + "}"
        );

        Consumer<String> checkPipelineExists = (id) -> assertThat(
            client().admin().cluster().prepareGetPipeline(id).get().pipelines().get(0).getId(),
            equalTo(id)
        );

        client().admin().cluster().preparePutPipeline(pipelineIdWithScript, pipelineWithScript, MediaTypeRegistry.JSON).get();
        client().admin().cluster().preparePutPipeline(pipelineIdWithoutScript, pipelineWithoutScript, MediaTypeRegistry.JSON).get();

        checkPipelineExists.accept(pipelineIdWithScript);
        checkPipelineExists.accept(pipelineIdWithoutScript);

        internalCluster().restartNode(internalCluster().getClusterManagerName(), new InternalTestCluster.RestartCallback() {

            @Override
            public Settings onNodeStopped(String nodeName) {
                return Settings.builder().put("script.allowed_types", "none").build();
            }

        });

        checkPipelineExists.accept(pipelineIdWithoutScript);
        checkPipelineExists.accept(pipelineIdWithScript);

        client().prepareIndex("index")
            .setId("1")
            .setSource("x", 0)
            .setPipeline(pipelineIdWithoutScript)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> client().prepareIndex("index")
                .setId("2")
                .setSource("x", 0)
                .setPipeline(pipelineIdWithScript)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get()
        );
        assertThat(
            exception.getMessage(),
            equalTo(
                "pipeline with id ["
                    + pipelineIdWithScript
                    + "] could not be loaded, caused by "
                    + "[OpenSearchParseException[Error updating pipeline with id ["
                    + pipelineIdWithScript
                    + "]]; "
                    + "nested: OpenSearchException[java.lang.IllegalArgumentException: cannot execute [inline] scripts]; "
                    + "nested: IllegalArgumentException[cannot execute [inline] scripts];; "
                    + "OpenSearchException[java.lang.IllegalArgumentException: cannot execute [inline] scripts]; "
                    + "nested: IllegalArgumentException[cannot execute [inline] scripts];; java.lang.IllegalArgumentException: "
                    + "cannot execute [inline] scripts]"
            )
        );

        Map<String, Object> source = client().prepareGet("index", "1").get().getSource();
        assertThat(source.get("x"), equalTo(0));
        assertThat(source.get("y"), equalTo(0));
    }

    public void testPipelineWithScriptProcessorThatHasStoredScript() throws Exception {
        internalCluster().startNode();

        client().admin()
            .cluster()
            .preparePutStoredScript()
            .setId("1")
            .setContent(
                new BytesArray("{\"script\": {\"lang\": \"" + MockScriptEngine.NAME + "\", \"source\": \"my_script\"} }"),
                MediaTypeRegistry.JSON
            )
            .get();
        BytesReference pipeline = new BytesArray(
            "{\n"
                + "  \"processors\" : [\n"
                + "      {\"set\" : {\"field\": \"y\", \"value\": 0}},\n"
                + "      {\"script\" : {\"id\": \"1\"}}\n"
                + "  ]\n"
                + "}"
        );
        client().admin().cluster().preparePutPipeline("_id", pipeline, MediaTypeRegistry.JSON).get();

        client().prepareIndex("index")
            .setId("1")
            .setSource("x", 0)
            .setPipeline("_id")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        Map<String, Object> source = client().prepareGet("index", "1").get().getSource();
        assertThat(source.get("x"), equalTo(0));
        assertThat(source.get("y"), equalTo(0));
        assertThat(source.get("z"), equalTo(0));

        // Prior to making this ScriptService implement ClusterStateApplier instead of ClusterStateListener,
        // pipelines with a script processor failed to load causing these pipelines and pipelines that were
        // supposed to load after these pipelines to not be available during ingestion, which then causes
        // the next index request in this test to fail.
        internalCluster().fullRestart();
        ensureYellow("index");

        client().prepareIndex("index")
            .setId("2")
            .setSource("x", 0)
            .setPipeline("_id")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        source = client().prepareGet("index", "2").get().getSource();
        assertThat(source.get("x"), equalTo(0));
        assertThat(source.get("y"), equalTo(0));
        assertThat(source.get("z"), equalTo(0));
    }

    public void testWithDedicatedIngestNode() throws Exception {
        String node = internalCluster().startNode();
        String ingestNode = internalCluster().startNode(Settings.builder().put("node.master", false).put("node.data", false));

        BytesReference pipeline = new BytesArray(
            "{\n" + "  \"processors\" : [\n" + "      {\"set\" : {\"field\": \"y\", \"value\": 0}}\n" + "  ]\n" + "}"
        );
        client().admin().cluster().preparePutPipeline("_id", pipeline, MediaTypeRegistry.JSON).get();

        client().prepareIndex("index")
            .setId("1")
            .setSource("x", 0)
            .setPipeline("_id")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        Map<String, Object> source = client().prepareGet("index", "1").get().getSource();
        assertThat(source.get("x"), equalTo(0));
        assertThat(source.get("y"), equalTo(0));

        logger.info("Stopping");
        internalCluster().restartNode(node, new InternalTestCluster.RestartCallback());

        client(ingestNode).prepareIndex("index")
            .setId("2")
            .setSource("x", 0)
            .setPipeline("_id")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        source = client(ingestNode).prepareGet("index", "2").get().getSource();
        assertThat(source.get("x"), equalTo(0));
        assertThat(source.get("y"), equalTo(0));
    }

}
