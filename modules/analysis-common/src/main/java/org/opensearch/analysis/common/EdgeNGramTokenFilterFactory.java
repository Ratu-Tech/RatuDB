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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.reverse.ReverseStringFilter;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AbstractTokenFilterFactory;
import org.opensearch.index.analysis.TokenFilterFactory;

public class EdgeNGramTokenFilterFactory extends AbstractTokenFilterFactory {
    private final int minGram;

    private final int maxGram;

    public static final int SIDE_FRONT = 1;
    public static final int SIDE_BACK = 2;
    private final int side;
    private final boolean preserveOriginal;
    private static final String PRESERVE_ORIG_KEY = "preserve_original";

    EdgeNGramTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        this.minGram = settings.getAsInt("min_gram", 1);
        this.maxGram = settings.getAsInt("max_gram", 2);
        this.side = parseSide(settings.get("side", "front"));
        this.preserveOriginal = settings.getAsBoolean(PRESERVE_ORIG_KEY, false);
    }

    static int parseSide(String side) {
        switch (side) {
            case "front":
                return SIDE_FRONT;
            case "back":
                return SIDE_BACK;
            default:
                throw new IllegalArgumentException("invalid side: " + side);
        }
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        TokenStream result = tokenStream;

        // side=BACK is not supported anymore but applying ReverseStringFilter up-front and after the token filter has the same effect
        if (side == SIDE_BACK) {
            result = new ReverseStringFilter(result);
        }

        result = new EdgeNGramTokenFilter(result, minGram, maxGram, preserveOriginal);

        // side=BACK is not supported anymore but applying ReverseStringFilter up-front and after the token filter has the same effect
        if (side == SIDE_BACK) {
            result = new ReverseStringFilter(result);
        }

        return result;
    }

    @Override
    public boolean breaksFastVectorHighlighter() {
        return true;
    }

    @Override
    public TokenFilterFactory getSynonymFilter() {
        throw new IllegalArgumentException("Token filter [" + name() + "] cannot be used to parse synonyms");
    }
}
