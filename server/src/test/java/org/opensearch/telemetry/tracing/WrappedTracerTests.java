/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.telemetry.TelemetrySettings;
import org.opensearch.telemetry.tracing.attributes.Attributes;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WrappedTracerTests extends OpenSearchTestCase {

    public void testStartSpanWithTracingDisabledInvokesNoopTracer() throws Exception {
        Settings settings = Settings.builder().put(TelemetrySettings.TRACER_ENABLED_SETTING.getKey(), false).build();
        TelemetrySettings telemetrySettings = new TelemetrySettings(settings, new ClusterSettings(settings, getClusterSettings()));
        DefaultTracer mockDefaultTracer = mock(DefaultTracer.class);

        try (WrappedTracer wrappedTracer = new WrappedTracer(telemetrySettings, mockDefaultTracer)) {
            SpanCreationContext spanCreationContext = SpanCreationContext.internal().name("foo");
            wrappedTracer.startSpan(spanCreationContext);
            assertTrue(wrappedTracer.getDelegateTracer() instanceof NoopTracer);
            assertFalse(wrappedTracer.isRecording());
            verify(mockDefaultTracer, never()).startSpan(SpanCreationContext.internal().name("foo"));
        }
    }

    public void testStartSpanWithTracingEnabledInvokesDefaultTracer() throws Exception {
        Settings settings = Settings.builder().put(TelemetrySettings.TRACER_ENABLED_SETTING.getKey(), true).build();
        TelemetrySettings telemetrySettings = new TelemetrySettings(settings, new ClusterSettings(settings, getClusterSettings()));
        DefaultTracer mockDefaultTracer = mock(DefaultTracer.class);
        when(mockDefaultTracer.isRecording()).thenReturn(true);
        try (WrappedTracer wrappedTracer = new WrappedTracer(telemetrySettings, mockDefaultTracer)) {
            SpanCreationContext spanCreationContext = SpanCreationContext.internal().name("foo");
            wrappedTracer.startSpan(spanCreationContext);

            assertTrue(wrappedTracer.getDelegateTracer() instanceof DefaultTracer);
            assertTrue(wrappedTracer.isRecording());
            verify(mockDefaultTracer).startSpan(eq(spanCreationContext));
        }
    }

    public void testStartSpanWithTracingEnabledInvokesDefaultTracerWithAttr() throws Exception {
        Settings settings = Settings.builder().put(TelemetrySettings.TRACER_ENABLED_SETTING.getKey(), true).build();
        TelemetrySettings telemetrySettings = new TelemetrySettings(settings, new ClusterSettings(settings, getClusterSettings()));
        DefaultTracer mockDefaultTracer = mock(DefaultTracer.class);
        Attributes attributes = Attributes.create().addAttribute("key", "value");
        try (WrappedTracer wrappedTracer = new WrappedTracer(telemetrySettings, mockDefaultTracer)) {
            SpanCreationContext spanCreationContext = SpanCreationContext.internal().name("foo");
            wrappedTracer.startSpan(spanCreationContext);

            assertTrue(wrappedTracer.getDelegateTracer() instanceof DefaultTracer);
            verify(mockDefaultTracer).startSpan(spanCreationContext);
        }
    }

    public void testClose() throws IOException {
        DefaultTracer mockDefaultTracer = mock(DefaultTracer.class);
        WrappedTracer wrappedTracer = new WrappedTracer(null, mockDefaultTracer);

        wrappedTracer.close();

        verify(mockDefaultTracer).close();
    }

    private Set<Setting<?>> getClusterSettings() {
        Set<Setting<?>> allTracerSettings = new HashSet<>();
        ClusterSettings.FEATURE_FLAGGED_CLUSTER_SETTINGS.get(List.of(FeatureFlags.TELEMETRY)).stream().forEach((allTracerSettings::add));
        return allTracerSettings;
    }
}
