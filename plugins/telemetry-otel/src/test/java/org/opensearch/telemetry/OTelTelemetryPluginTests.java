/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.telemetry.metrics.MetricsTelemetry;
import org.opensearch.telemetry.metrics.OTelMetricsTelemetry;
import org.opensearch.telemetry.tracing.OTelTracingTelemetry;
import org.opensearch.telemetry.tracing.TracingTelemetry;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.opensearch.telemetry.OTelTelemetryPlugin.OTEL_TRACER_NAME;
import static org.opensearch.telemetry.OTelTelemetrySettings.OTEL_METRICS_EXPORTER_CLASS_SETTING;
import static org.opensearch.telemetry.OTelTelemetrySettings.OTEL_TRACER_SPAN_EXPORTER_CLASS_SETTING;
import static org.opensearch.telemetry.OTelTelemetrySettings.TRACER_EXPORTER_BATCH_SIZE_SETTING;
import static org.opensearch.telemetry.OTelTelemetrySettings.TRACER_EXPORTER_DELAY_SETTING;
import static org.opensearch.telemetry.OTelTelemetrySettings.TRACER_EXPORTER_MAX_QUEUE_SIZE_SETTING;
import static org.opensearch.telemetry.TelemetrySettings.TRACER_ENABLED_SETTING;
import static org.opensearch.telemetry.TelemetrySettings.TRACER_SAMPLER_PROBABILITY;

public class OTelTelemetryPluginTests extends OpenSearchTestCase {

    private OTelTelemetryPlugin oTelTelemetryPlugin;
    private Optional<Telemetry> telemetry;
    private TracingTelemetry tracingTelemetry;

    private MetricsTelemetry metricsTelemetry;

    @Before
    public void setup() {
        // TRACER_EXPORTER_DELAY_SETTING should always be less than 10 seconds because
        // io.opentelemetry.sdk.OpenTelemetrySdk.close waits only for 10 seconds for shutdown to complete.
        Settings settings = Settings.builder().put(TRACER_EXPORTER_DELAY_SETTING.getKey(), "1s").build();
        oTelTelemetryPlugin = new OTelTelemetryPlugin(settings);
        telemetry = oTelTelemetryPlugin.getTelemetry(
            new TelemetrySettings(Settings.EMPTY, new ClusterSettings(settings, Set.of(TRACER_ENABLED_SETTING, TRACER_SAMPLER_PROBABILITY)))
        );
        tracingTelemetry = telemetry.get().getTracingTelemetry();
        metricsTelemetry = telemetry.get().getMetricsTelemetry();
    }

    public void testGetTelemetry() {
        Set<Setting<?>> allTracerSettings = new HashSet<>();
        ClusterSettings.FEATURE_FLAGGED_CLUSTER_SETTINGS.get(List.of(FeatureFlags.TELEMETRY)).stream().forEach((allTracerSettings::add));
        assertEquals(OTEL_TRACER_NAME, oTelTelemetryPlugin.getName());
        assertTrue(tracingTelemetry instanceof OTelTracingTelemetry);
        assertTrue(metricsTelemetry instanceof OTelMetricsTelemetry);
        assertEquals(
            Arrays.asList(
                TRACER_EXPORTER_BATCH_SIZE_SETTING,
                TRACER_EXPORTER_DELAY_SETTING,
                TRACER_EXPORTER_MAX_QUEUE_SIZE_SETTING,
                OTEL_TRACER_SPAN_EXPORTER_CLASS_SETTING,
                OTEL_METRICS_EXPORTER_CLASS_SETTING
            ),
            oTelTelemetryPlugin.getSettings()
        );

    }

    @After
    public void cleanup() throws IOException {
        tracingTelemetry.close();
        metricsTelemetry.close();
    }
}
