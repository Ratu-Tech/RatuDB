/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.telemetry;

import org.opensearch.telemetry.Telemetry;
import org.opensearch.telemetry.TelemetrySettings;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.MetricsTelemetry;
import org.opensearch.telemetry.metrics.noop.NoopCounter;
import org.opensearch.telemetry.tracing.TracingTelemetry;
import org.opensearch.test.telemetry.tracing.MockTracingTelemetry;

/**
 * Mock {@link Telemetry} implementation for testing.
 */
public class MockTelemetry implements Telemetry {
    /**
     * Constructor with settings.
     * @param settings telemetry settings.
     */
    public MockTelemetry(TelemetrySettings settings) {

    }

    @Override
    public TracingTelemetry getTracingTelemetry() {
        return new MockTracingTelemetry();
    }

    @Override
    public MetricsTelemetry getMetricsTelemetry() {
        return new MetricsTelemetry() {
            @Override
            public Counter createCounter(String name, String description, String unit) {
                return NoopCounter.INSTANCE;
            }

            @Override
            public Counter createUpDownCounter(String name, String description, String unit) {
                return NoopCounter.INSTANCE;
            }

            @Override
            public void close() {

            }
        };
    }
}
