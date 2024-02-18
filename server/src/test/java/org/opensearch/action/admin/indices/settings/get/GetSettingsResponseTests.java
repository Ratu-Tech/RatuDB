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

package org.opensearch.action.admin.indices.settings.get;

import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.RandomCreateIndexGenerator;
import org.opensearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

public class GetSettingsResponseTests extends AbstractSerializingTestCase<GetSettingsResponse> {

    @Override
    protected GetSettingsResponse createTestInstance() {
        HashMap<String, Settings> indexToSettings = new HashMap<>();
        HashMap<String, Settings> indexToDefaultSettings = new HashMap<>();

        IndexScopedSettings indexScopedSettings = IndexScopedSettings.DEFAULT_SCOPED_SETTINGS;

        Set<String> indexNames = new HashSet<String>();
        int numIndices = randomIntBetween(1, 5);
        for (int x = 0; x < numIndices; x++) {
            String indexName = randomAlphaOfLength(5);
            indexNames.add(indexName);
        }

        for (String indexName : indexNames) {
            Settings.Builder builder = Settings.builder();
            builder.put(RandomCreateIndexGenerator.randomIndexSettings());
            /*
            We must ensure that *something* is in the settings response as we optimize away empty settings
            blocks in x content responses
             */
            builder.put("index.refresh_interval", "1s");
            indexToSettings.put(indexName, builder.build());
        }
        final Map<String, Settings> immutableIndexToSettings = Collections.unmodifiableMap(indexToSettings);

        if (randomBoolean()) {
            for (String indexName : indexToSettings.keySet()) {
                Settings defaultSettings = indexScopedSettings.diff(indexToSettings.get(indexName), Settings.EMPTY);
                indexToDefaultSettings.put(indexName, defaultSettings);
            }
        }

        final Map<String, Settings> immutableIndexToDefaultSettings = Collections.unmodifiableMap(indexToDefaultSettings);

        return new GetSettingsResponse(immutableIndexToSettings, immutableIndexToDefaultSettings);
    }

    @Override
    protected Writeable.Reader<GetSettingsResponse> instanceReader() {
        return GetSettingsResponse::new;
    }

    @Override
    protected GetSettingsResponse doParseInstance(XContentParser parser) throws IOException {
        return GetSettingsResponse.fromXContent(parser);
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // we do not want to add new fields at the root (index-level), or inside settings blocks
        return f -> f.equals("") || f.contains(".settings") || f.contains(".defaults");
    }
}
