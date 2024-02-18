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

import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchParseException;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;
import org.opensearch.ingest.RandomDocumentPicks;
import org.hamcrest.CoreMatchers;

public class BytesProcessorTests extends AbstractStringProcessorTestCase<Long> {

    private String modifiedInput;

    @Override
    protected AbstractStringProcessor<Long> newProcessor(String field, boolean ignoreMissing, String targetField) {
        return new BytesProcessor(randomAlphaOfLength(10), null, field, ignoreMissing, targetField);
    }

    @Override
    protected String modifyInput(String input) {
        // largest value that allows all results < Long.MAX_VALUE bytes
        long randomNumber = randomLongBetween(1, Long.MAX_VALUE / ByteSizeUnit.PB.toBytes(1));
        ByteSizeUnit randomUnit = randomFrom(ByteSizeUnit.values());
        modifiedInput = randomNumber + randomUnit.getSuffix();
        return modifiedInput;
    }

    @Override
    protected Long expectedResult(String input) {
        return ByteSizeValue.parseBytesSizeValue(modifiedInput, null, "").getBytes();
    }

    @Override
    protected Class<Long> expectedResultType() {
        return Long.class;
    }

    public void testTooLarge() {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "8912pb");
        Processor processor = newProcessor(fieldName, randomBoolean(), fieldName);
        OpenSearchException exception = expectThrows(OpenSearchException.class, () -> processor.execute(ingestDocument));
        assertThat(
            exception.getMessage(),
            CoreMatchers.equalTo("failed to parse setting [Ingest Field] with value [8912pb] as a size in bytes")
        );
        assertThat(
            exception.getCause().getMessage(),
            CoreMatchers.containsString("Values greater than 9223372036854775807 bytes are not supported")
        );
    }

    public void testNotBytes() {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "junk");
        Processor processor = newProcessor(fieldName, randomBoolean(), fieldName);
        OpenSearchException exception = expectThrows(OpenSearchException.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), CoreMatchers.equalTo("failed to parse [junk]"));
    }

    public void testMissingUnits() {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "1");
        Processor processor = newProcessor(fieldName, randomBoolean(), fieldName);
        OpenSearchException exception = expectThrows(OpenSearchException.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), CoreMatchers.containsString("unit is missing or unrecognized"));
    }

    public void testFractional() {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "1.1kb");
        Processor processor = newProcessor(fieldName, randomBoolean(), fieldName);
        OpenSearchParseException e = expectThrows(OpenSearchParseException.class, () -> processor.execute(ingestDocument));
        assertThat(
            e.getMessage(),
            CoreMatchers.containsString(
                "Fractional bytes values have been deprecated since Legacy 6.2. " + "Use non-fractional bytes values instead:"
            )
        );
    }
}
