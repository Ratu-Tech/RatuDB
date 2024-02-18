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
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.opensearch.Version;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.analysis.AnalysisTestsHelper;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.analysis.TokenFilterFactory;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.OpenSearchTokenStreamTestCase;
import org.opensearch.test.VersionUtils;

import java.io.IOException;
import java.io.StringReader;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.hamcrest.Matchers.instanceOf;
import static com.carrotsearch.randomizedtesting.RandomizedTest.scaledRandomIntBetween;

public class StemmerTokenFilterFactoryTests extends OpenSearchTokenStreamTestCase {

    private static final CommonAnalysisModulePlugin PLUGIN = new CommonAnalysisModulePlugin();

    public void testEnglishFilterFactory() throws IOException {
        int iters = scaledRandomIntBetween(20, 100);
        for (int i = 0; i < iters; i++) {
            Version v = VersionUtils.randomVersion(random());
            Settings settings = Settings.builder()
                .put("index.analysis.filter.my_english.type", "stemmer")
                .put("index.analysis.filter.my_english.language", "english")
                .put("index.analysis.analyzer.my_english.tokenizer", "whitespace")
                .put("index.analysis.analyzer.my_english.filter", "my_english")
                .put(SETTING_VERSION_CREATED, v)
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();

            OpenSearchTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, PLUGIN);
            TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_english");
            assertThat(tokenFilter, instanceOf(StemmerTokenFilterFactory.class));
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader("foo bar"));
            TokenStream create = tokenFilter.create(tokenizer);
            IndexAnalyzers indexAnalyzers = analysis.indexAnalyzers;
            NamedAnalyzer analyzer = indexAnalyzers.get("my_english");
            assertThat(create, instanceOf(PorterStemFilter.class));
            assertAnalyzesTo(analyzer, "consolingly", new String[] { "consolingli" });
        }
    }

    public void testPorter2FilterFactory() throws IOException {
        int iters = scaledRandomIntBetween(20, 100);
        for (int i = 0; i < iters; i++) {

            Version v = VersionUtils.randomVersion(random());
            Settings settings = Settings.builder()
                .put("index.analysis.filter.my_porter2.type", "stemmer")
                .put("index.analysis.filter.my_porter2.language", "porter2")
                .put("index.analysis.analyzer.my_porter2.tokenizer", "whitespace")
                .put("index.analysis.analyzer.my_porter2.filter", "my_porter2")
                .put(SETTING_VERSION_CREATED, v)
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();

            OpenSearchTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, PLUGIN);
            TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_porter2");
            assertThat(tokenFilter, instanceOf(StemmerTokenFilterFactory.class));
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader("foo bar"));
            TokenStream create = tokenFilter.create(tokenizer);
            IndexAnalyzers indexAnalyzers = analysis.indexAnalyzers;
            NamedAnalyzer analyzer = indexAnalyzers.get("my_porter2");
            assertThat(create, instanceOf(SnowballFilter.class));
            assertAnalyzesTo(analyzer, "possibly", new String[] { "possibl" });
        }
    }

    public void testEnglishPluralFilter() throws IOException {
        int iters = scaledRandomIntBetween(20, 100);
        for (int i = 0; i < iters; i++) {

            Version v = VersionUtils.randomVersion(random());
            Settings settings = Settings.builder()
                .put("index.analysis.filter.my_plurals.type", "stemmer")
                .put("index.analysis.filter.my_plurals.language", "plural_english")
                .put("index.analysis.analyzer.my_plurals.tokenizer", "whitespace")
                .put("index.analysis.analyzer.my_plurals.filter", "my_plurals")
                .put(SETTING_VERSION_CREATED, v)
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();

            OpenSearchTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, PLUGIN);
            TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_plurals");
            assertThat(tokenFilter, instanceOf(StemmerTokenFilterFactory.class));
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader("dresses"));
            TokenStream create = tokenFilter.create(tokenizer);
            IndexAnalyzers indexAnalyzers = analysis.indexAnalyzers;
            NamedAnalyzer analyzer = indexAnalyzers.get("my_plurals");
            assertThat(create, instanceOf(EnglishPluralStemFilter.class));

            // Check old EnglishMinimalStemmer ("S" stemmer) logic
            assertAnalyzesTo(analyzer, "phones", new String[] { "phone" });
            assertAnalyzesTo(analyzer, "horses", new String[] { "horse" });
            assertAnalyzesTo(analyzer, "cameras", new String[] { "camera" });

            // The orginal s stemmer gives up on stemming oes words because English has no fixed rule for the stem
            // (see https://howtospell.co.uk/making-O-words-plural )
            // This stemmer removes the es but retains e for a small number of exceptions
            assertAnalyzesTo(analyzer, "mosquitoes", new String[] { "mosquito" });
            assertAnalyzesTo(analyzer, "heroes", new String[] { "hero" });
            // oes exceptions that retain the e.
            assertAnalyzesTo(analyzer, "shoes", new String[] { "shoe" });
            assertAnalyzesTo(analyzer, "horseshoes", new String[] { "horseshoe" });
            assertAnalyzesTo(analyzer, "canoes", new String[] { "canoe" });
            assertAnalyzesTo(analyzer, "oboes", new String[] { "oboe" });

            // Check improved EnglishPluralStemFilter logic
            // sses
            assertAnalyzesTo(analyzer, "dresses", new String[] { "dress" });
            assertAnalyzesTo(analyzer, "possess", new String[] { "possess" });
            assertAnalyzesTo(analyzer, "possesses", new String[] { "possess" });
            // xes
            assertAnalyzesTo(analyzer, "boxes", new String[] { "box" });
            assertAnalyzesTo(analyzer, "axes", new String[] { "axe" });
            // shes
            assertAnalyzesTo(analyzer, "dishes", new String[] { "dish" });
            assertAnalyzesTo(analyzer, "washes", new String[] { "wash" });
            // ees
            assertAnalyzesTo(analyzer, "employees", new String[] { "employee" });
            assertAnalyzesTo(analyzer, "bees", new String[] { "bee" });
            // tch
            assertAnalyzesTo(analyzer, "watches", new String[] { "watch" });
            assertAnalyzesTo(analyzer, "itches", new String[] { "itch" });
            // ies->y but only for length >4
            assertAnalyzesTo(analyzer, "spies", new String[] { "spy" });
            assertAnalyzesTo(analyzer, "ties", new String[] { "tie" });
            assertAnalyzesTo(analyzer, "lies", new String[] { "lie" });
            assertAnalyzesTo(analyzer, "pies", new String[] { "pie" });
            assertAnalyzesTo(analyzer, "dies", new String[] { "die" });

            assertAnalyzesTo(analyzer, "lunches", new String[] { "lunch" });
            assertAnalyzesTo(analyzer, "avalanches", new String[] { "avalanche" });
            assertAnalyzesTo(analyzer, "headaches", new String[] { "headache" });
            assertAnalyzesTo(analyzer, "caches", new String[] { "cache" });
            assertAnalyzesTo(analyzer, "beaches", new String[] { "beach" });
            assertAnalyzesTo(analyzer, "britches", new String[] { "britch" });
            assertAnalyzesTo(analyzer, "cockroaches", new String[] { "cockroach" });
            assertAnalyzesTo(analyzer, "cliches", new String[] { "cliche" });
            assertAnalyzesTo(analyzer, "quiches", new String[] { "quiche" });

        }
    }

    public void testMultipleLanguagesThrowsException() throws IOException {
        Version v = VersionUtils.randomVersion(random());
        Settings settings = Settings.builder()
            .put("index.analysis.filter.my_english.type", "stemmer")
            .putList("index.analysis.filter.my_english.language", "english", "light_english")
            .put(SETTING_VERSION_CREATED, v)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> AnalysisTestsHelper.createTestAnalysisFromSettings(settings, PLUGIN)
        );
        assertEquals("Invalid stemmer class specified: [english, light_english]", e.getMessage());
    }
}
