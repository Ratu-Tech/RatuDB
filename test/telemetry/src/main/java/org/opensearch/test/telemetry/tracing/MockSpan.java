/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.telemetry.tracing;

import org.opensearch.telemetry.tracing.AbstractSpan;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanCreationContext;
import org.opensearch.telemetry.tracing.SpanKind;
import org.opensearch.telemetry.tracing.attributes.Attributes;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

/**
 * MockSpan for testing and strict check validations. Not to be used for production cases.
 */
public class MockSpan extends AbstractSpan {
    private final SpanProcessor spanProcessor;
    private final Map<String, Object> metadata;
    private final String traceId;
    private final String spanId;
    private boolean hasEnded;
    private final Long startTime;
    private Long endTime;
    private final SpanKind spanKind;

    private final Object lock = new Object();

    private static final Supplier<Random> randomSupplier = ThreadLocalRandom::current;

    /**
     * Base Constructor.
     *
     * @param spanCreationContext      Span Creation context.
     * @param parentSpan    Parent Span
     * @param spanProcessor Span Processor
     */
    public MockSpan(SpanCreationContext spanCreationContext, Span parentSpan, SpanProcessor spanProcessor) {
        this(
            spanCreationContext.getSpanName(),
            parentSpan,
            parentSpan != null ? parentSpan.getTraceId() : IdGenerator.generateTraceId(),
            IdGenerator.generateSpanId(),
            spanProcessor,
            spanCreationContext.getAttributes(),
            SpanKind.INTERNAL
        );
    }

    /**
     * Constructor with traceId and SpanIds
     *
     * @param spanName      Span Name
     * @param parentSpan    Parent Span
     * @param traceId       Trace ID
     * @param spanId        Span ID
     * @param spanProcessor Span Processor
     * @param attributes    attributes
     * @param spanKind      type of span.
     */
    public MockSpan(
        String spanName,
        Span parentSpan,
        String traceId,
        String spanId,
        SpanProcessor spanProcessor,
        Attributes attributes,
        SpanKind spanKind
    ) {
        super(spanName, parentSpan);
        this.spanProcessor = spanProcessor;
        this.metadata = new HashMap<>();
        this.traceId = traceId;
        this.spanId = spanId;
        this.startTime = System.nanoTime();
        if (attributes != null) {
            this.metadata.putAll(attributes.getAttributesMap());
        }
        this.spanKind = spanKind;
    }

    @Override
    public void endSpan() {
        synchronized (lock) {
            if (hasEnded) {
                return;
            }
            endTime = System.nanoTime();
            hasEnded = true;
        }
        spanProcessor.onEnd(this);
    }

    @Override
    public void addAttribute(String key, String value) {
        putMetadata(key, value);
    }

    @Override
    public void addAttribute(String key, Long value) {
        putMetadata(key, value);
    }

    @Override
    public void addAttribute(String key, Double value) {
        putMetadata(key, value);
    }

    @Override
    public void addAttribute(String key, Boolean value) {
        putMetadata(key, value);
    }

    @Override
    public void addEvent(String event) {
        putMetadata(event, null);
    }

    private void putMetadata(String key, Object value) {
        metadata.put(key, value);
    }

    @Override
    public String getTraceId() {
        return traceId;
    }

    @Override
    public String getSpanId() {
        return spanId;
    }

    /**
     * Returns whether the span is ended or not.
     * @return span end status.
     */
    public boolean hasEnded() {
        synchronized (lock) {
            return hasEnded;
        }
    }

    /**
     * Returns the start time of the span.
     * @return start time of the span.
     */
    public Long getStartTime() {
        return startTime;
    }

    /**
     * Returns the start time of the span.
     * @return end time of the span.
     */
    public Long getEndTime() {
        return endTime;
    }

    public void setError(Exception exception) {
        if (exception != null) {
            putMetadata("ERROR", exception.getMessage());
        }
    }

    private static class IdGenerator {
        private static String generateSpanId() {
            long id = randomSupplier.get().nextLong();
            return Long.toHexString(id);
        }

        private static String generateTraceId() {
            long idHi = randomSupplier.get().nextLong();
            long idLo = randomSupplier.get().nextLong();
            long result = idLo | (idHi << 32);
            return Long.toHexString(result);
        }

    }

    /**
     * Returns attribute.
     * @param key key
     * @return value
     */
    public Object getAttribute(String key) {
        return metadata.get(key);
    }

    /**
     * Returns the attributes as map.
     * @return returns the attributes map.
     */
    public Map<String, Object> getAttributes() {
        return metadata;
    }
}
