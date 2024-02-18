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

package org.opensearch.indices.settings;

import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;

public class InternalSettingsIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(InternalOrPrivateSettingsPlugin.class);
    }

    public void testSetInternalIndexSettingOnCreate() {
        final Settings settings = Settings.builder().put("index.internal", "internal").build();
        createIndex("index", settings);
        final GetSettingsResponse response = client().admin().indices().prepareGetSettings("index").get();
        assertThat(response.getSetting("index", "index.internal"), equalTo("internal"));
    }

    public void testUpdateInternalIndexSettingViaSettingsAPI() {
        final Settings settings = Settings.builder().put("index.internal", "internal").build();
        createIndex("test", settings);
        final GetSettingsResponse response = client().admin().indices().prepareGetSettings("test").get();
        assertThat(response.getSetting("test", "index.internal"), equalTo("internal"));
        // we can not update the setting via the update settings API
        final SettingsException e = expectThrows(
            SettingsException.class,
            () -> client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().put("index.internal", "internal-update"))
                .get()
        );
        final String message = "can not update internal setting [index.internal]; this setting is managed via a dedicated API";
        assertThat(e, hasToString(containsString(message)));
        final GetSettingsResponse responseAfterAttemptedUpdate = client().admin().indices().prepareGetSettings("test").get();
        assertThat(responseAfterAttemptedUpdate.getSetting("test", "index.internal"), equalTo("internal"));
    }

    public void testUpdateInternalIndexSettingViaDedicatedAPI() {
        final Settings settings = Settings.builder().put("index.internal", "internal").build();
        createIndex("test", settings);
        final GetSettingsResponse response = client().admin().indices().prepareGetSettings("test").get();
        assertThat(response.getSetting("test", "index.internal"), equalTo("internal"));
        client().execute(
            InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.INSTANCE,
            new InternalOrPrivateSettingsPlugin.UpdateInternalOrPrivateAction.Request("test", "index.internal", "internal-update")
        ).actionGet();
        final GetSettingsResponse responseAfterUpdate = client().admin().indices().prepareGetSettings("test").get();
        assertThat(responseAfterUpdate.getSetting("test", "index.internal"), equalTo("internal-update"));
    }

}
