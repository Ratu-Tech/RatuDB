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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.test;

import org.opensearch.common.CheckedBiConsumer;
import org.opensearch.common.CheckedBiFunction;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentHelper;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertToXContentEquivalent;

public abstract class AbstractXContentTestCase<T extends ToXContent> extends OpenSearchTestCase {
    protected static final int NUMBER_OF_TEST_RUNS = 20;

    public static <T> XContentTester<T> xContentTester(
        CheckedBiFunction<XContent, BytesReference, XContentParser, IOException> createParser,
        Supplier<T> instanceSupplier,
        CheckedBiConsumer<T, XContentBuilder, IOException> toXContent,
        CheckedFunction<XContentParser, T, IOException> fromXContent
    ) {
        return new XContentTester<>(createParser, x -> instanceSupplier.get(), (testInstance, xContentType) -> {
            try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
                toXContent.accept(testInstance, builder);
                return BytesReference.bytes(builder);
            }
        }, fromXContent);
    }

    public static <T extends ToXContent> XContentTester<T> xContentTester(
        CheckedBiFunction<XContent, BytesReference, XContentParser, IOException> createParser,
        Supplier<T> instanceSupplier,
        CheckedFunction<XContentParser, T, IOException> fromXContent
    ) {
        return xContentTester(createParser, instanceSupplier, ToXContent.EMPTY_PARAMS, fromXContent);
    }

    public static <T extends ToXContent> XContentTester<T> xContentTester(
        CheckedBiFunction<XContent, BytesReference, XContentParser, IOException> createParser,
        Supplier<T> instanceSupplier,
        ToXContent.Params toXContentParams,
        CheckedFunction<XContentParser, T, IOException> fromXContent
    ) {
        return new XContentTester<>(
            createParser,
            x -> instanceSupplier.get(),
            (testInstance, xContentType) -> XContentHelper.toXContent(testInstance, xContentType, toXContentParams, false),
            fromXContent
        );
    }

    public static <T extends ToXContent> XContentTester<T> xContentTester(
        CheckedBiFunction<XContent, BytesReference, XContentParser, IOException> createParser,
        Function<MediaType, T> instanceSupplier,
        ToXContent.Params toXContentParams,
        CheckedFunction<XContentParser, T, IOException> fromXContent
    ) {
        return new XContentTester<>(
            createParser,
            instanceSupplier,
            (testInstance, xContentType) -> XContentHelper.toXContent(testInstance, xContentType, toXContentParams, false),
            fromXContent
        );
    }

    /**
     * Tests converting to and from xcontent.
     */
    public static class XContentTester<T> {
        private final CheckedBiFunction<XContent, BytesReference, XContentParser, IOException> createParser;
        private final Function<MediaType, T> instanceSupplier;
        private final CheckedBiFunction<T, MediaType, BytesReference, IOException> toXContent;
        private final CheckedFunction<XContentParser, T, IOException> fromXContent;

        private int numberOfTestRuns = NUMBER_OF_TEST_RUNS;
        private boolean supportsUnknownFields = false;
        private String[] shuffleFieldsExceptions = Strings.EMPTY_ARRAY;
        private Predicate<String> randomFieldsExcludeFilter = field -> false;
        private BiConsumer<T, T> assertEqualsConsumer = (expectedInstance, newInstance) -> {
            assertNotSame(newInstance, expectedInstance);
            assertEquals(expectedInstance, newInstance);
            assertEquals(expectedInstance.hashCode(), newInstance.hashCode());
        };
        private boolean assertToXContentEquivalence = true;

        private XContentTester(
            CheckedBiFunction<XContent, BytesReference, XContentParser, IOException> createParser,
            Function<MediaType, T> instanceSupplier,
            CheckedBiFunction<T, MediaType, BytesReference, IOException> toXContent,
            CheckedFunction<XContentParser, T, IOException> fromXContent
        ) {
            this.createParser = createParser;
            this.instanceSupplier = instanceSupplier;
            this.toXContent = toXContent;
            this.fromXContent = fromXContent;
        }

        public void test() throws IOException {
            for (int runs = 0; runs < numberOfTestRuns; runs++) {
                XContentType xContentType = randomFrom(XContentType.values());
                T testInstance = instanceSupplier.apply(xContentType);
                BytesReference originalXContent = toXContent.apply(testInstance, xContentType);
                BytesReference shuffledContent = insertRandomFieldsAndShuffle(
                    originalXContent,
                    xContentType,
                    supportsUnknownFields,
                    shuffleFieldsExceptions,
                    randomFieldsExcludeFilter,
                    createParser
                );
                XContentParser parser = createParser.apply(xContentType.xContent(), shuffledContent);
                T parsed = fromXContent.apply(parser);
                assertEqualsConsumer.accept(testInstance, parsed);
                if (assertToXContentEquivalence) {
                    assertToXContentEquivalent(
                        toXContent.apply(testInstance, xContentType),
                        toXContent.apply(parsed, xContentType),
                        xContentType
                    );
                }
            }
        }

        public XContentTester<T> numberOfTestRuns(int numberOfTestRuns) {
            this.numberOfTestRuns = numberOfTestRuns;
            return this;
        }

        public XContentTester<T> supportsUnknownFields(boolean supportsUnknownFields) {
            this.supportsUnknownFields = supportsUnknownFields;
            return this;
        }

        public XContentTester<T> shuffleFieldsExceptions(String[] shuffleFieldsExceptions) {
            this.shuffleFieldsExceptions = shuffleFieldsExceptions;
            return this;
        }

        public XContentTester<T> randomFieldsExcludeFilter(Predicate<String> randomFieldsExcludeFilter) {
            this.randomFieldsExcludeFilter = randomFieldsExcludeFilter;
            return this;
        }

        public XContentTester<T> assertEqualsConsumer(BiConsumer<T, T> assertEqualsConsumer) {
            this.assertEqualsConsumer = assertEqualsConsumer;
            return this;
        }

        public XContentTester<T> assertToXContentEquivalence(boolean assertToXContentEquivalence) {
            this.assertToXContentEquivalence = assertToXContentEquivalence;
            return this;
        }
    }

    public static <T extends ToXContent> void testFromXContent(
        int numberOfTestRuns,
        Supplier<T> instanceSupplier,
        boolean supportsUnknownFields,
        String[] shuffleFieldsExceptions,
        Predicate<String> randomFieldsExcludeFilter,
        CheckedBiFunction<XContent, BytesReference, XContentParser, IOException> createParserFunction,
        CheckedFunction<XContentParser, T, IOException> fromXContent,
        BiConsumer<T, T> assertEqualsConsumer,
        boolean assertToXContentEquivalence,
        ToXContent.Params toXContentParams
    ) throws IOException {
        xContentTester(createParserFunction, instanceSupplier, toXContentParams, fromXContent).numberOfTestRuns(numberOfTestRuns)
            .supportsUnknownFields(supportsUnknownFields)
            .shuffleFieldsExceptions(shuffleFieldsExceptions)
            .randomFieldsExcludeFilter(randomFieldsExcludeFilter)
            .assertEqualsConsumer(assertEqualsConsumer)
            .assertToXContentEquivalence(assertToXContentEquivalence)
            .test();
    }

    /**
     * Generic test that creates new instance from the test instance and checks
     * both for equality and asserts equality on the two queries.
     */
    public final void testFromXContent() throws IOException {
        testFromXContent(
            NUMBER_OF_TEST_RUNS,
            this::createTestInstance,
            supportsUnknownFields(),
            getShuffleFieldsExceptions(),
            getRandomFieldsExcludeFilter(),
            this::createParser,
            this::parseInstance,
            this::assertEqualInstances,
            assertToXContentEquivalence(),
            getToXContentParams()
        );
    }

    /**
     * Creates a random test instance to use in the tests. This method will be
     * called multiple times during test execution and should return a different
     * random instance each time it is called.
     */
    protected abstract T createTestInstance();

    private T parseInstance(XContentParser parser) throws IOException {
        T parsedInstance = doParseInstance(parser);
        assertNull(parser.nextToken());
        return parsedInstance;
    }

    /**
     * Parses to a new instance using the provided {@link XContentParser}
     */
    protected abstract T doParseInstance(XContentParser parser) throws IOException;

    protected void assertEqualInstances(T expectedInstance, T newInstance) {
        assertNotSame(newInstance, expectedInstance);
        assertEquals(expectedInstance, newInstance);
        assertEquals(expectedInstance.hashCode(), newInstance.hashCode());
    }

    protected boolean assertToXContentEquivalence() {
        return true;
    }

    /**
     * Indicates whether the parser supports unknown fields or not. In case it does, such behaviour will be tested by
     * inserting random fields before parsing and checking that they don't make parsing fail.
     */
    protected abstract boolean supportsUnknownFields();

    /**
     * Returns a predicate that given the field name indicates whether the field has to be excluded from random fields insertion or not
     */
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> false;
    }

    /**
     * Fields that have to be ignored when shuffling as part of testFromXContent
     */
    protected String[] getShuffleFieldsExceptions() {
        return Strings.EMPTY_ARRAY;
    }

    /**
     * Params that have to be provided when calling {@link ToXContent#toXContent(XContentBuilder, ToXContent.Params)}
     */
    protected ToXContent.Params getToXContentParams() {
        return ToXContent.EMPTY_PARAMS;
    }

    static BytesReference insertRandomFieldsAndShuffle(
        BytesReference xContent,
        MediaType mediaType,
        boolean supportsUnknownFields,
        String[] shuffleFieldsExceptions,
        Predicate<String> randomFieldsExcludeFilter,
        CheckedBiFunction<XContent, BytesReference, XContentParser, IOException> createParserFunction
    ) throws IOException {
        BytesReference withRandomFields;
        if (supportsUnknownFields) {
            // add a few random fields to check that the parser is lenient on new fields
            withRandomFields = XContentTestUtils.insertRandomFields(mediaType, xContent, randomFieldsExcludeFilter, random());
        } else {
            withRandomFields = xContent;
        }
        XContentParser parserWithRandonFields = createParserFunction.apply(mediaType.xContent(), withRandomFields);
        return BytesReference.bytes(shuffleXContent(parserWithRandonFields, false, shuffleFieldsExceptions));
    }

}
