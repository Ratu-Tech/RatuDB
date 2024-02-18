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

package org.opensearch.ingest.common;

import org.opensearch.OpenSearchParseException;
import org.opensearch.ingest.TestTemplateService;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DateProcessorFactoryTests extends OpenSearchTestCase {

    private DateProcessor.Factory factory;

    @Before
    public void init() {
        factory = new DateProcessor.Factory(TestTemplateService.instance());
    }

    public void testBuildDefaults() throws Exception {
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("formats", Collections.singletonList("dd/MM/yyyyy"));
        String processorTag = randomAlphaOfLength(10);
        DateProcessor processor = factory.create(null, processorTag, null, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo(sourceField));
        assertThat(processor.getTargetField(), equalTo(DateProcessor.DEFAULT_TARGET_FIELD));
        assertThat(processor.getFormats(), equalTo(Collections.singletonList("dd/MM/yyyyy")));
        assertNull(processor.getLocale());
        assertNull(processor.getTimezone());
    }

    public void testMatchFieldIsMandatory() throws Exception {
        Map<String, Object> config = new HashMap<>();
        String targetField = randomAlphaOfLengthBetween(1, 10);
        config.put("target_field", targetField);
        config.put("formats", Collections.singletonList("dd/MM/yyyyy"));

        try {
            factory.create(null, null, null, config);
            fail("processor creation should have failed");
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), containsString("[field] required property is missing"));
        }
    }

    public void testMatchFormatsIsMandatory() throws Exception {
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        String targetField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("target_field", targetField);

        try {
            factory.create(null, null, null, config);
            fail("processor creation should have failed");
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), containsString("[formats] required property is missing"));
        }
    }

    public void testParseLocale() throws Exception {
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("formats", Collections.singletonList("dd/MM/yyyyy"));
        Locale locale = randomFrom(Locale.GERMANY, Locale.FRENCH, Locale.ROOT);
        config.put("locale", locale.toLanguageTag());

        DateProcessor processor = factory.create(null, null, null, config);
        assertThat(processor.getLocale().newInstance(Collections.emptyMap()).execute(), equalTo(locale.toLanguageTag()));
    }

    public void testParseTimezone() throws Exception {
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("formats", Collections.singletonList("dd/MM/yyyyy"));

        ZoneId timezone = randomZone();
        config.put("timezone", timezone.getId());
        DateProcessor processor = factory.create(null, null, null, config);
        assertThat(processor.getTimezone().newInstance(Collections.emptyMap()).execute(), equalTo(timezone.getId()));
    }

    public void testParseMatchFormats() throws Exception {
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("formats", Arrays.asList("dd/MM/yyyy", "dd-MM-yyyy"));

        DateProcessor processor = factory.create(null, null, null, config);
        assertThat(processor.getFormats(), equalTo(Arrays.asList("dd/MM/yyyy", "dd-MM-yyyy")));
    }

    public void testParseMatchFormatsFailure() throws Exception {
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("formats", "dd/MM/yyyy");

        try {
            factory.create(null, null, null, config);
            fail("processor creation should have failed");
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), containsString("[formats] property isn't a list, but of type [java.lang.String]"));
        }
    }

    public void testParseTargetField() throws Exception {
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        String targetField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("target_field", targetField);
        config.put("formats", Arrays.asList("dd/MM/yyyy", "dd-MM-yyyy"));

        DateProcessor processor = factory.create(null, null, null, config);
        assertThat(processor.getTargetField(), equalTo(targetField));
    }

    public void testParseOutputFormat() throws Exception {
        final String outputFormat = "dd:MM:yyyy";
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        String targetField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("target_field", targetField);
        config.put("formats", Arrays.asList("dd/MM/yyyy", "dd-MM-yyyy"));
        config.put("output_format", outputFormat);
        DateProcessor processor = factory.create(null, null, null, config);
        assertThat(processor.getOutputFormat(), equalTo(outputFormat));
    }

    public void testDefaultOutputFormat() throws Exception {
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        String targetField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("target_field", targetField);
        config.put("formats", Arrays.asList("dd/MM/yyyy", "dd-MM-yyyy"));
        DateProcessor processor = factory.create(null, null, null, config);
        assertThat(processor.getOutputFormat(), equalTo(DateProcessor.DEFAULT_OUTPUT_FORMAT));
    }

    public void testInvalidOutputFormatRejected() throws Exception {
        final String outputFormat = "invalid_date_format";
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        String targetField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("target_field", targetField);
        config.put("formats", Arrays.asList("dd/MM/yyyy", "dd-MM-yyyy"));
        config.put("output_format", outputFormat);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> factory.create(null, null, null, config));
        assertThat(e.getMessage(), containsString("invalid output format [" + outputFormat + "]"));
    }
}
