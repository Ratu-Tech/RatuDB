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

package org.opensearch.upgrades;

import org.opensearch.LegacyESVersion;
import org.opensearch.client.Request;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.index.IndexSettings;
import org.junit.Before;

import java.io.IOException;
import java.util.Locale;

/**
 * Ensures that we correctly trim unsafe commits when migrating from a translog generation to the sequence number based policy.
 * See https://github.com/elastic/elasticsearch/issues/57091
 */
public class TranslogPolicyIT extends AbstractFullClusterRestartTestCase {

    private enum TestStep {
        STEP1_OLD_CLUSTER("step1"),
        STEP2_OLD_CLUSTER("step2"),
        STEP3_NEW_CLUSTER("step3"),
        STEP4_NEW_CLUSTER("step4");

        private final String name;

        TestStep(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }

        public static TestStep parse(String value) {
            switch (value) {
                case "step1":
                    return STEP1_OLD_CLUSTER;
                case "step2":
                    return STEP2_OLD_CLUSTER;
                case "step3":
                    return STEP3_NEW_CLUSTER;
                case "step4":
                    return STEP4_NEW_CLUSTER;
                default:
                    throw new AssertionError("unknown test step: " + value);
            }
        }
    }

    protected static final TestStep TEST_STEP = TestStep.parse(System.getProperty("tests.test_step"));

    private String index;
    private String type;

    @Before
    public void setIndex() {
        index = getTestName().toLowerCase(Locale.ROOT);
    }

    @Before
    public void setType() {
        type = "_doc";
    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/pull/2225")
    public void testEmptyIndex() throws Exception {
        if (TEST_STEP == TestStep.STEP1_OLD_CLUSTER) {
            final Settings.Builder settings = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, between(0, 1));
            settings.put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), randomBoolean());
            if (randomBoolean()) {
                settings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), "-1");
            }
            createIndex(index, settings.build());
        }
        ensureGreen(index);
        assertTotalHits(0, entityAsMap(client().performRequest(new Request("GET", "/" + index + "/_search"))));
    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/pull/2225")
    public void testRecoverReplica() throws Exception {
        int numDocs = 100;
        if (TEST_STEP == TestStep.STEP1_OLD_CLUSTER) {
            final Settings.Builder settings = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1);
            settings.put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), randomBoolean());
            if (randomBoolean()) {
                settings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), "-1");
            }
            if (randomBoolean()) {
                settings.put(IndexSettings.INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING.getKey(), "1kb");
            }
            createIndex(index, settings.build());
            ensureGreen(index);
            for (int i = 0; i < numDocs; i++) {
                indexDocument(Integer.toString(i));
                if (rarely()) {
                    flush(index, randomBoolean());
                }
            }
            client().performRequest(new Request("POST", "/" + index + "/_refresh"));
            if (randomBoolean()) {
                ensurePeerRecoveryRetentionLeasesRenewedAndSynced(index);
            }
            if (randomBoolean()) {
                flush(index, randomBoolean());
            } else if (randomBoolean()) {
                syncedFlush(index, randomBoolean());
            }
        }
        ensureGreen(index);
        assertTotalHits(100, entityAsMap(client().performRequest(new Request("GET", "/" + index + "/_search"))));
    }

    private void indexDocument(String id) throws IOException {
        final Request indexRequest = new Request("POST", "/" + index + "/" + type + "/" + id);
        indexRequest.setJsonEntity(Strings.toString(JsonXContent.contentBuilder().startObject().field("f", "v").endObject()));
        assertOK(client().performRequest(indexRequest));
    }
}
