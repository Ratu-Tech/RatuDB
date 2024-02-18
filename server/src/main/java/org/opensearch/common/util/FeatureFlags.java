/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;

/**
 * Utility class to manage feature flags. Feature flags are system properties that must be set on the JVM.
 * These are used to gate the visibility/availability of incomplete features. Fore more information, see
 * https://featureflags.io/feature-flag-introduction/
 *
 * @opensearch.internal
 */
public class FeatureFlags {
    /**
     * Gates the visibility of the segment replication experimental features that allows users to test unreleased beta features.
     */
    public static final String SEGMENT_REPLICATION_EXPERIMENTAL =
        "opensearch.experimental.feature.segment_replication_experimental.enabled";

    /**
     * Gates the ability for Searchable Snapshots to read snapshots that are older than the
     * guaranteed backward compatibility for OpenSearch (one prior major version) on a best effort basis.
     */
    public static final String SEARCHABLE_SNAPSHOT_EXTENDED_COMPATIBILITY =
        "opensearch.experimental.feature.searchable_snapshot.extended_compatibility.enabled";

    /**
     * Gates the functionality of extensions.
     * Once the feature is ready for production release, this feature flag can be removed.
     */
    public static final String EXTENSIONS = "opensearch.experimental.feature.extensions.enabled";

    /**
     * Gates the functionality of identity.
     */
    public static final String IDENTITY = "opensearch.experimental.feature.identity.enabled";

    /**
     * Gates the functionality of concurrently searching the segments
     * Once the feature is ready for release, this feature flag can be removed.
     */
    public static final String CONCURRENT_SEGMENT_SEARCH = "opensearch.experimental.feature.concurrent_segment_search.enabled";

    /**
     * Gates the functionality of telemetry framework.
     */
    public static final String TELEMETRY = "opensearch.experimental.feature.telemetry.enabled";

    /**
     * Gates the optimization of datetime formatters caching along with change in default datetime formatter.
     */
    public static final String DATETIME_FORMATTER_CACHING = "opensearch.experimental.optimization.datetime_formatter_caching.enabled";

    /**
     * Should store the settings from opensearch.yml.
     */
    private static Settings settings;

    /**
     * This method is responsible to map settings from opensearch.yml to local stored
     * settings value. That is used for the existing isEnabled method.
     *
     * @param openSearchSettings The settings stored in opensearch.yml.
     */
    public static void initializeFeatureFlags(Settings openSearchSettings) {
        settings = openSearchSettings;
    }

    /**
     * Used to test feature flags whose values are expected to be booleans.
     * This method returns true if the value is "true" (case-insensitive),
     * and false otherwise.
     */
    public static boolean isEnabled(String featureFlagName) {
        if ("true".equalsIgnoreCase(System.getProperty(featureFlagName))) {
            // TODO: Remove the if condition once FeatureFlags are only supported via opensearch.yml
            return true;
        }
        return settings != null && settings.getAsBoolean(featureFlagName, false);
    }

    public static boolean isEnabled(Setting<Boolean> featureFlag) {
        if ("true".equalsIgnoreCase(System.getProperty(featureFlag.getKey()))) {
            // TODO: Remove the if condition once FeatureFlags are only supported via opensearch.yml
            return true;
        } else if (settings != null) {
            return featureFlag.get(settings);
        } else {
            return featureFlag.getDefault(Settings.EMPTY);
        }
    }

    public static final Setting<Boolean> SEGMENT_REPLICATION_EXPERIMENTAL_SETTING = Setting.boolSetting(
        SEGMENT_REPLICATION_EXPERIMENTAL,
        false,
        Property.NodeScope
    );

    public static final Setting<Boolean> EXTENSIONS_SETTING = Setting.boolSetting(EXTENSIONS, false, Property.NodeScope);

    public static final Setting<Boolean> IDENTITY_SETTING = Setting.boolSetting(IDENTITY, false, Property.NodeScope);

    public static final Setting<Boolean> TELEMETRY_SETTING = Setting.boolSetting(TELEMETRY, false, Property.NodeScope);

    public static final Setting<Boolean> CONCURRENT_SEGMENT_SEARCH_SETTING = Setting.boolSetting(
        CONCURRENT_SEGMENT_SEARCH,
        false,
        Property.NodeScope
    );

    public static final Setting<Boolean> DATETIME_FORMATTER_CACHING_SETTING = Setting.boolSetting(
        DATETIME_FORMATTER_CACHING,
        true,
        Property.NodeScope
    );
}
