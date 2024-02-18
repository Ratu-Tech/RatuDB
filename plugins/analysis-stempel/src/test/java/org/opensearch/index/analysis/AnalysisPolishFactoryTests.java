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

package org.opensearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.pl.PolishStemTokenFilterFactory;
import org.opensearch.indices.analysis.AnalysisFactoryTestCase;
import org.opensearch.plugin.analysis.stempel.AnalysisStempelPlugin;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AnalysisPolishFactoryTests extends AnalysisFactoryTestCase {
    public AnalysisPolishFactoryTests() {
        super(new AnalysisStempelPlugin());
    }

    @Override
    protected Map<String, Class<?>> getTokenFilters() {
        Map<String, Class<?>> filters = new HashMap<>(super.getTokenFilters());
        filters.put("stempelpolishstem", PolishStemTokenFilterFactory.class);
        return filters;
    }

    public void testThreadSafety() throws IOException {
        // TODO: is this the right boilerplate? I forked this out of TransportAnalyzeAction.java:
        Settings settings = Settings.builder()
            // for _na_
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        Environment environment = TestEnvironment.newEnvironment(settings);
        IndexMetadata metadata = IndexMetadata.builder(IndexMetadata.INDEX_UUID_NA_VALUE).settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(metadata, Settings.EMPTY);
        testThreadSafety(new PolishStemTokenFilterFactory(indexSettings, environment, "stempelpolishstem", settings));
    }

    // TODO: move to AnalysisFactoryTestCase so we can more easily test thread safety for all factories
    private void testThreadSafety(TokenFilterFactory factory) throws IOException {
        final Analyzer analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer tokenizer = new MockTokenizer();
                return new TokenStreamComponents(tokenizer, factory.create(tokenizer));
            }
        };
        BaseTokenStreamTestCase.checkRandomData(random(), analyzer, 100);
    }
}
