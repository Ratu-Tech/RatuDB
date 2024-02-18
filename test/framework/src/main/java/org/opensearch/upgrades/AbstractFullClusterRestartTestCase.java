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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import org.opensearch.Version;
import org.opensearch.common.Booleans;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.util.Map;

public abstract class AbstractFullClusterRestartTestCase extends OpenSearchRestTestCase {

    private static final boolean runningAgainstOldCluster = Booleans.parseBoolean(System.getProperty("tests.is_old_cluster"));

    public static boolean isRunningAgainstOldCluster() {
        return runningAgainstOldCluster;
    }

    private static final Version oldClusterVersion = Version.fromString(System.getProperty("tests.old_cluster_version"));

    /**
     * @return true if test is running against an old cluster before that last major, in this case
     * when System.getProperty("tests.is_old_cluster" == true) and oldClusterVersion is before {@link Version#V_2_0_0}
     */
    protected final boolean isRunningAgainstAncientCluster() {
        return isRunningAgainstOldCluster() && oldClusterVersion.before(Version.V_2_0_0);
    }

    public static Version getOldClusterVersion() {
        return oldClusterVersion;
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveSnapshotsUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveReposUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveTemplatesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveClusterSettings() {
        return true;
    }

    @Override
    protected boolean preserveSLMPoliciesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveDataStreamsUponCompletion() {
        return true;
    }

    protected static void assertNoFailures(Map<?, ?> response) {
        int failed = (int) XContentMapValues.extractValue("_shards.failed", response);
        assertEquals(0, failed);
    }

    protected void assertTotalHits(int expectedTotalHits, Map<?, ?> response) {
        int actualTotalHits = extractTotalHits(response);
        assertEquals(response.toString(), expectedTotalHits, actualTotalHits);
    }

    protected static int extractTotalHits(Map<?, ?> response) {
        return (Integer) XContentMapValues.extractValue("hits.total.value", response);
    }
}
