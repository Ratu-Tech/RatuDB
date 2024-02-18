/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.test.telemetry.tracing.MockSpanData;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;

public class InMemorySingletonSpanExporter implements SpanExporter {

    public static final InMemorySingletonSpanExporter INSTANCE = new InMemorySingletonSpanExporter(InMemorySpanExporter.create());

    private static InMemorySpanExporter delegate;

    public static InMemorySingletonSpanExporter create() {
        return INSTANCE;
    }

    private InMemorySingletonSpanExporter(InMemorySpanExporter delegate) {
        InMemorySingletonSpanExporter.delegate = delegate;
    }

    @Override
    public CompletableResultCode export(Collection<SpanData> spans) {
        return delegate.export(spans);
    }

    @Override
    public CompletableResultCode flush() {
        return delegate.flush();
    }

    @Override
    public CompletableResultCode shutdown() {
        return delegate.shutdown();
    }

    public List<MockSpanData> getFinishedSpanItems() {
        return convertSpanDataListToMockSpanDataList(delegate.getFinishedSpanItems());
    }

    private List<MockSpanData> convertSpanDataListToMockSpanDataList(List<SpanData> spanDataList) {
        List<MockSpanData> mockSpanDataList = spanDataList.stream()
            .map(
                spanData -> new MockSpanData(
                    spanData.getSpanId(),
                    spanData.getParentSpanId(),
                    spanData.getTraceId(),
                    spanData.getStartEpochNanos(),
                    spanData.getEndEpochNanos(),
                    spanData.hasEnded(),
                    spanData.getName(),
                    getAttributes(spanData)
                )
            )
            .collect(Collectors.toList());
        return mockSpanDataList;
    }

    private Map<String, Object> getAttributes(SpanData spanData) {
        if (spanData.getAttributes() != null) {
            return spanData.getAttributes()
                .asMap()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(e -> e.getKey().getKey(), e -> e.getValue()));
        } else {
            return Collections.emptyMap();
        }
    }

    /**
     * Clears the state.
     */
    public void reset() {
        delegate.reset();
    }
}
