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

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.PatternKeywordMarkerFilter;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AbstractTokenFilterFactory;
import org.opensearch.index.analysis.Analysis;

import java.util.Set;
import java.util.regex.Pattern;

/**
 * A factory for creating keyword marker token filters that prevent tokens from
 * being modified by stemmers.  Two types of keyword marker filters are available:
 * the {@link SetKeywordMarkerFilter} and the {@link PatternKeywordMarkerFilter}.
 * <p>
 * The {@link SetKeywordMarkerFilter} uses a set of keywords to denote which tokens
 * should be excluded from stemming.  This filter is created if the settings include
 * {@code keywords}, which contains the list of keywords, or {@code `keywords_path`},
 * which contains a path to a file in the config directory with the keywords.
 * <p>
 * The {@link PatternKeywordMarkerFilter} uses a regular expression pattern to match
 * against tokens that should be excluded from stemming.  This filter is created if
 * the settings include {@code keywords_pattern}, which contains the regular expression
 * to match against.
 */
public class KeywordMarkerTokenFilterFactory extends AbstractTokenFilterFactory {

    private final CharArraySet keywordLookup;
    private final Pattern keywordPattern;

    KeywordMarkerTokenFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, name, settings);

        boolean ignoreCase = settings.getAsBoolean("ignore_case", false);
        String patternString = settings.get("keywords_pattern");
        if (patternString != null) {
            // a pattern for matching keywords is specified, as opposed to a
            // set of keyword strings to match against
            if (settings.get("keywords") != null || settings.get("keywords_path") != null) {
                throw new IllegalArgumentException("cannot specify both `keywords_pattern` and `keywords` or `keywords_path`");
            }
            keywordPattern = Pattern.compile(patternString);
            keywordLookup = null;
        } else {
            Set<?> rules = Analysis.getWordSet(env, settings, "keywords");
            if (rules == null) {
                throw new IllegalArgumentException(
                    "keyword filter requires either `keywords`, `keywords_path`, " + "or `keywords_pattern` to be configured"
                );
            }
            // a set of keywords (or a path to them) is specified
            keywordLookup = new CharArraySet(rules, ignoreCase);
            keywordPattern = null;
        }
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        if (keywordPattern != null) {
            return new PatternKeywordMarkerFilter(tokenStream, keywordPattern);
        } else {
            return new SetKeywordMarkerFilter(tokenStream, keywordLookup);
        }
    }

}
