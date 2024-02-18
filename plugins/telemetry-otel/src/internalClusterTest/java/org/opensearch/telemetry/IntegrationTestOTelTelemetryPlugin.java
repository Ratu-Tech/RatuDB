/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry;

import org.opensearch.common.settings.Settings;

import java.util.Optional;

import io.opentelemetry.api.GlobalOpenTelemetry;

/**
 * Telemetry plugin used for Integration tests.
*/
public class IntegrationTestOTelTelemetryPlugin extends OTelTelemetryPlugin {
    /**
     * Creates IntegrationTestOTelTelemetryPlugin
     * @param settings cluster settings
     */
    public IntegrationTestOTelTelemetryPlugin(Settings settings) {
        super(settings);
    }

    /**
     * This method overrides getTelemetry() method in OTel plugin class, so we create only one instance of global OpenTelemetry
     * resetForTest() will set OpenTelemetry to null again.
     * @param telemetrySettings telemetry settings
     */
    public Optional<Telemetry> getTelemetry(TelemetrySettings telemetrySettings) {
        GlobalOpenTelemetry.resetForTest();
        return super.getTelemetry(telemetrySettings);
    }
}
