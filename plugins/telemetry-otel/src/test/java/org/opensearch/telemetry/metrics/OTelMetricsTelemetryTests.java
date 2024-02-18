/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.metrics;

import org.opensearch.common.concurrent.RefCountedReleasable;
import org.opensearch.telemetry.OTelAttributesConverter;
import org.opensearch.telemetry.OTelTelemetryPlugin;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.test.OpenSearchTestCase;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.DoubleCounter;
import io.opentelemetry.api.metrics.DoubleCounterBuilder;
import io.opentelemetry.api.metrics.DoubleUpDownCounter;
import io.opentelemetry.api.metrics.DoubleUpDownCounterBuilder;
import io.opentelemetry.api.metrics.LongCounterBuilder;
import io.opentelemetry.api.metrics.LongUpDownCounterBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.MeterProvider;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OTelMetricsTelemetryTests extends OpenSearchTestCase {

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testCounter() {
        String counterName = "test-counter";
        String description = "test";
        String unit = "1";
        Meter mockMeter = mock(Meter.class);
        OpenTelemetry mockOpenTelemetry = mock(OpenTelemetry.class);
        DoubleCounter mockOTelDoubleCounter = mock(DoubleCounter.class);
        LongCounterBuilder mockOTelLongCounterBuilder = mock(LongCounterBuilder.class);
        DoubleCounterBuilder mockOTelDoubleCounterBuilder = mock(DoubleCounterBuilder.class);
        MeterProvider meterProvider = mock(MeterProvider.class);
        when(meterProvider.get(OTelTelemetryPlugin.INSTRUMENTATION_SCOPE_NAME)).thenReturn(mockMeter);
        MetricsTelemetry metricsTelemetry = new OTelMetricsTelemetry(
            new RefCountedReleasable("telemetry", mockOpenTelemetry, () -> {}),
            meterProvider
        );
        when(mockMeter.counterBuilder(counterName)).thenReturn(mockOTelLongCounterBuilder);
        when(mockOTelLongCounterBuilder.setDescription(description)).thenReturn(mockOTelLongCounterBuilder);
        when(mockOTelLongCounterBuilder.setUnit(unit)).thenReturn(mockOTelLongCounterBuilder);
        when(mockOTelLongCounterBuilder.ofDoubles()).thenReturn(mockOTelDoubleCounterBuilder);
        when(mockOTelDoubleCounterBuilder.build()).thenReturn(mockOTelDoubleCounter);

        Counter counter = metricsTelemetry.createCounter(counterName, description, unit);
        counter.add(1.0);
        verify(mockOTelDoubleCounter).add(1.0);
        Tags tags = Tags.create().addTag("test", "test");
        counter.add(2.0, tags);
        verify(mockOTelDoubleCounter).add(2.0, OTelAttributesConverter.convert(tags));
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testCounterNegativeValue() {
        String counterName = "test-counter";
        String description = "test";
        String unit = "1";
        OpenTelemetry mockOpenTelemetry = mock(OpenTelemetry.class);
        Meter mockMeter = mock(Meter.class);
        DoubleCounter mockOTelDoubleCounter = mock(DoubleCounter.class);
        LongCounterBuilder mockOTelLongCounterBuilder = mock(LongCounterBuilder.class);
        DoubleCounterBuilder mockOTelDoubleCounterBuilder = mock(DoubleCounterBuilder.class);

        MeterProvider meterProvider = mock(MeterProvider.class);
        when(meterProvider.get(OTelTelemetryPlugin.INSTRUMENTATION_SCOPE_NAME)).thenReturn(mockMeter);
        MetricsTelemetry metricsTelemetry = new OTelMetricsTelemetry(
            new RefCountedReleasable("telemetry", mockOpenTelemetry, () -> {}),
            meterProvider
        );
        when(mockMeter.counterBuilder(counterName)).thenReturn(mockOTelLongCounterBuilder);
        when(mockOTelLongCounterBuilder.setDescription(description)).thenReturn(mockOTelLongCounterBuilder);
        when(mockOTelLongCounterBuilder.setUnit(unit)).thenReturn(mockOTelLongCounterBuilder);
        when(mockOTelLongCounterBuilder.ofDoubles()).thenReturn(mockOTelDoubleCounterBuilder);
        when(mockOTelDoubleCounterBuilder.build()).thenReturn(mockOTelDoubleCounter);

        Counter counter = metricsTelemetry.createCounter(counterName, description, unit);
        counter.add(-1.0);
        verify(mockOTelDoubleCounter).add(-1.0);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testUpDownCounter() {
        String counterName = "test-counter";
        String description = "test";
        String unit = "1";
        OpenTelemetry mockOpenTelemetry = mock(OpenTelemetry.class);
        Meter mockMeter = mock(Meter.class);
        DoubleUpDownCounter mockOTelUpDownDoubleCounter = mock(DoubleUpDownCounter.class);
        LongUpDownCounterBuilder mockOTelLongUpDownCounterBuilder = mock(LongUpDownCounterBuilder.class);
        DoubleUpDownCounterBuilder mockOTelDoubleUpDownCounterBuilder = mock(DoubleUpDownCounterBuilder.class);

        MeterProvider meterProvider = mock(MeterProvider.class);
        when(meterProvider.get(OTelTelemetryPlugin.INSTRUMENTATION_SCOPE_NAME)).thenReturn(mockMeter);
        MetricsTelemetry metricsTelemetry = new OTelMetricsTelemetry(
            new RefCountedReleasable("telemetry", mockOpenTelemetry, () -> {}),
            meterProvider
        );
        when(mockMeter.upDownCounterBuilder(counterName)).thenReturn(mockOTelLongUpDownCounterBuilder);
        when(mockOTelLongUpDownCounterBuilder.setDescription(description)).thenReturn(mockOTelLongUpDownCounterBuilder);
        when(mockOTelLongUpDownCounterBuilder.setUnit(unit)).thenReturn(mockOTelLongUpDownCounterBuilder);
        when(mockOTelLongUpDownCounterBuilder.ofDoubles()).thenReturn(mockOTelDoubleUpDownCounterBuilder);
        when(mockOTelDoubleUpDownCounterBuilder.build()).thenReturn(mockOTelUpDownDoubleCounter);

        Counter counter = metricsTelemetry.createUpDownCounter(counterName, description, unit);
        counter.add(1.0);
        verify(mockOTelUpDownDoubleCounter).add(1.0);
        Tags tags = Tags.create().addTag("test", "test");
        counter.add(-2.0, tags);
        verify(mockOTelUpDownDoubleCounter).add((-2.0), OTelAttributesConverter.convert(tags));
    }
}
