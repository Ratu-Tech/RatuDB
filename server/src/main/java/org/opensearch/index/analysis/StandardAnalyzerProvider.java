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

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;

/**
 * Provider for the standard analyzer
 *
 * @opensearch.internal
 */
public class StandardAnalyzerProvider extends AbstractIndexAnalyzerProvider<StandardAnalyzer> {

    private final StandardAnalyzer standardAnalyzer;

    public StandardAnalyzerProvider(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, name, settings);
        final CharArraySet defaultStopwords = CharArraySet.EMPTY_SET;
        CharArraySet stopWords = Analysis.parseStopWords(env, settings, defaultStopwords);
        int maxTokenLength = settings.getAsInt("max_token_length", StandardAnalyzer.DEFAULT_MAX_TOKEN_LENGTH);
        standardAnalyzer = new StandardAnalyzer(stopWords);
        standardAnalyzer.setMaxTokenLength(maxTokenLength);
    }

    @Override
    public StandardAnalyzer get() {
        return this.standardAnalyzer;
    }
}
