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

package org.opensearch.analysis.common;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.ParseField;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AbstractIndexAnalyzerProvider;
import org.opensearch.index.analysis.Analysis;

/**
 * Builds an OpenRefine Fingerprint analyzer.  Uses the default settings from the various components
 * (Standard Tokenizer and lowercase + stop + fingerprint + ascii-folding filters)
 */
public class FingerprintAnalyzerProvider extends AbstractIndexAnalyzerProvider<Analyzer> {

    public static ParseField SEPARATOR = new ParseField("separator");
    public static ParseField MAX_OUTPUT_SIZE = new ParseField("max_output_size");

    public static int DEFAULT_MAX_OUTPUT_SIZE = 255;
    public static CharArraySet DEFAULT_STOP_WORDS = CharArraySet.EMPTY_SET;
    public static final char DEFAULT_SEPARATOR = ' ';

    private final FingerprintAnalyzer analyzer;

    FingerprintAnalyzerProvider(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, name, settings);

        char separator = parseSeparator(settings);
        int maxOutputSize = settings.getAsInt(MAX_OUTPUT_SIZE.getPreferredName(), DEFAULT_MAX_OUTPUT_SIZE);
        CharArraySet stopWords = Analysis.parseStopWords(env, settings, DEFAULT_STOP_WORDS);

        this.analyzer = new FingerprintAnalyzer(stopWords, separator, maxOutputSize);
    }

    @Override
    public FingerprintAnalyzer get() {
        return analyzer;
    }

    public static char parseSeparator(Settings settings) throws IllegalArgumentException {
        String customSeparator = settings.get(SEPARATOR.getPreferredName());
        if (customSeparator == null) {
            return DEFAULT_SEPARATOR;
        } else if (customSeparator.length() == 1) {
            return customSeparator.charAt(0);
        }

        throw new IllegalArgumentException(
            "Setting [separator] must be a single, non-null character. [" + customSeparator + "] was provided."
        );
    }
}
