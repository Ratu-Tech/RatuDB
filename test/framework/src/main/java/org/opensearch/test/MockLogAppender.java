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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.filter.RegexFilter;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.regex.Regex;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test appender that can be used to verify that certain events were logged correctly
 */
public class MockLogAppender extends AbstractAppender implements AutoCloseable {

    private static final String COMMON_PREFIX = System.getProperty("opensearch.logger.prefix", "org.opensearch.");

    private final List<LoggingExpectation> expectations;
    private final List<Logger> loggers;

    /**
     * Creates an instance and adds it as an appender to the given Loggers. Upon
     * closure, this instance will then remove itself from the Loggers it was added
     * to. It is strongly recommended to use this class in a try-with-resources block
     * to guarantee that it is properly removed from all Loggers. Since the logging
     * state is static and therefore global within a JVM, it can cause unrelated
     * tests to fail if, for example, they trigger a logging statement that tried to
     * write to a closed MockLogAppender instance.
     */
    public static MockLogAppender createForLoggers(Logger... loggers) throws IllegalAccessException {
        return createForLoggers(".*(\n.*)*", loggers);
    }

    public static MockLogAppender createForLoggers(String filter, Logger... loggers) throws IllegalAccessException {
        final MockLogAppender appender = new MockLogAppender(
            RegexFilter.createFilter(filter, new String[0], false, null, null),
            Collections.unmodifiableList(Arrays.asList(loggers))
        );
        appender.start();
        for (Logger logger : loggers) {
            Loggers.addAppender(logger, appender);
        }
        return appender;
    }

    private MockLogAppender(RegexFilter filter, List<Logger> loggers) {
        super("mock", filter, null);
        /*
         * We use a copy-on-write array list since log messages could be appended while we are setting up expectations. When that occurs,
         * we would run into a concurrent modification exception from the iteration over the expectations in #append, concurrent with a
         * modification from #addExpectation.
         */
        this.expectations = new CopyOnWriteArrayList<>();
        this.loggers = loggers;
    }

    public void addExpectation(LoggingExpectation expectation) {
        expectations.add(expectation);
    }

    @Override
    public void append(LogEvent event) {
        for (LoggingExpectation expectation : expectations) {
            expectation.match(event);
        }
    }

    public void assertAllExpectationsMatched() {
        for (LoggingExpectation expectation : expectations) {
            expectation.assertMatched();
        }
    }

    @Override
    public void close() {
        for (Logger logger : loggers) {
            Loggers.removeAppender(logger, this);
        }
        this.stop();
    }

    public interface LoggingExpectation {
        void match(LogEvent event);

        void assertMatched();
    }

    public abstract static class AbstractEventExpectation implements LoggingExpectation {
        protected final String name;
        protected final String logger;
        protected final Level level;
        protected final String message;
        volatile boolean saw;

        public AbstractEventExpectation(String name, String logger, Level level, String message) {
            this.name = name;
            this.logger = getLoggerName(logger);
            this.level = level;
            this.message = message;
            this.saw = false;
        }

        @Override
        public void match(LogEvent event) {
            if (event.getLevel().equals(level) && event.getLoggerName().equals(logger) && innerMatch(event)) {
                if (Regex.isSimpleMatchPattern(message)) {
                    if (Regex.simpleMatch(message, event.getMessage().getFormattedMessage())) {
                        saw = true;
                    }
                } else {
                    if (event.getMessage().getFormattedMessage().contains(message)) {
                        saw = true;
                    }
                }
            }
        }

        public boolean innerMatch(final LogEvent event) {
            return true;
        }

    }

    public static class UnseenEventExpectation extends AbstractEventExpectation {

        public UnseenEventExpectation(String name, String logger, Level level, String message) {
            super(name, logger, level, message);
        }

        @Override
        public void assertMatched() {
            assertThat("expected not to see " + name + " but did", saw, equalTo(false));
        }
    }

    public static class SeenEventExpectation extends AbstractEventExpectation {

        public SeenEventExpectation(String name, String logger, Level level, String message) {
            super(name, logger, level, message);
        }

        @Override
        public void assertMatched() {
            assertThat("expected to see " + name + " but did not", saw, equalTo(true));
        }
    }

    public static class ExceptionSeenEventExpectation extends SeenEventExpectation {

        private final Class<? extends Exception> clazz;
        private final String exceptionMessage;

        public ExceptionSeenEventExpectation(
            final String name,
            final String logger,
            final Level level,
            final String message,
            final Class<? extends Exception> clazz,
            final String exceptionMessage
        ) {
            super(name, logger, level, message);
            this.clazz = clazz;
            this.exceptionMessage = exceptionMessage;
        }

        @Override
        public boolean innerMatch(final LogEvent event) {
            return event.getThrown() != null
                && event.getThrown().getClass() == clazz
                && event.getThrown().getMessage().equals(exceptionMessage);
        }

    }

    public static class PatternSeenEventExpectation implements LoggingExpectation {

        protected final String name;
        protected final String logger;
        protected final Level level;
        protected final String pattern;
        volatile boolean saw;

        public PatternSeenEventExpectation(String name, String logger, Level level, String pattern) {
            this.name = name;
            this.logger = logger;
            this.level = level;
            this.pattern = pattern;
        }

        @Override
        public void match(LogEvent event) {
            if (event.getLevel().equals(level) && event.getLoggerName().equals(logger)) {
                if (Pattern.matches(pattern, event.getMessage().getFormattedMessage())) {
                    saw = true;
                }
            }
        }

        @Override
        public void assertMatched() {
            assertThat(name, saw, equalTo(true));
        }

    }

    private static String getLoggerName(String name) {
        if (name.startsWith("org.opensearch.")) {
            name = name.substring("org.opensearch.".length());
        }
        return COMMON_PREFIX + name;
    }
}
