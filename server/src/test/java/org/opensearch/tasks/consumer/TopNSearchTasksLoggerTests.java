/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks.consumer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.logging.MockAppender;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.tasks.resourcetracker.ResourceStats;
import org.opensearch.core.tasks.resourcetracker.ResourceStatsType;
import org.opensearch.core.tasks.resourcetracker.ResourceUsageMetric;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Collections;

import static org.opensearch.tasks.consumer.TopNSearchTasksLogger.LOG_TOP_QUERIES_FREQUENCY_SETTING;
import static org.opensearch.tasks.consumer.TopNSearchTasksLogger.LOG_TOP_QUERIES_SIZE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

public class TopNSearchTasksLoggerTests extends OpenSearchSingleNodeTestCase {
    static MockAppender appender;
    static Logger searchLogger = LogManager.getLogger(TopNSearchTasksLogger.TASK_DETAILS_LOG_PREFIX + ".search");

    private TopNSearchTasksLogger topNSearchTasksLogger;

    @BeforeClass
    public static void init() throws IllegalAccessException {
        appender = new MockAppender("trace_appender");
        appender.start();
        Loggers.addAppender(searchLogger, appender);
    }

    @After
    public void cleanupAfterTest() {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putNull("*"))
                .setTransientSettings(Settings.builder().putNull("*"))
        );
    }

    @AfterClass
    public static void cleanup() {
        Loggers.removeAppender(searchLogger, appender);
        appender.stop();
    }

    public void testLoggerWithTasks() {
        final Settings settings = Settings.builder()
            .put(LOG_TOP_QUERIES_SIZE_SETTING.getKey(), 1)
            .put(LOG_TOP_QUERIES_FREQUENCY_SETTING.getKey(), "60s")
            .build();
        topNSearchTasksLogger = new TopNSearchTasksLogger(
            settings,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        // This setting overrides is just for testing purpose
        topNSearchTasksLogger.setTopQueriesLogFrequencyInNanos(TimeValue.timeValueMillis(0));
        generateTasks(5);
        LogEvent logEvent = appender.getLastEventAndReset();
        assertNotNull(logEvent);
        assertEquals(logEvent.getLevel(), Level.INFO);
        assertTrue(logEvent.getMessage().getFormattedMessage().contains("cpu_time_in_nanos=300, memory_in_bytes=300"));
    }

    public void testLoggerWithoutTasks() {
        final Settings settings = Settings.builder()
            .put(LOG_TOP_QUERIES_SIZE_SETTING.getKey(), 1)
            .put(LOG_TOP_QUERIES_FREQUENCY_SETTING.getKey(), "60s")
            .build();
        topNSearchTasksLogger = new TopNSearchTasksLogger(
            settings,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        assertNull(appender.getLastEventAndReset());
    }

    public void testLoggerWithHighFrequency() {
        // setting the frequency to a really large value and confirming that nothing gets written to log file.
        final Settings settings = Settings.builder()
            .put(LOG_TOP_QUERIES_SIZE_SETTING.getKey(), 1)
            .put(LOG_TOP_QUERIES_FREQUENCY_SETTING.getKey(), "10m")
            .build();
        topNSearchTasksLogger = new TopNSearchTasksLogger(
            settings,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        generateTasks(5);
        generateTasks(2);

        assertNull(appender.getLastEventAndReset());
    }

    // generate search tasks and updates the topN search tasks logger consumer.
    public void generateTasks(int numberOfTasks) {
        for (int i = 0; i < numberOfTasks; i++) {
            Task task = new SearchShardTask(
                i,
                "n/a",
                "n/a",
                "test",
                null,
                Collections.singletonMap(Task.X_OPAQUE_ID, "my_id"),
                () -> "n/a"
            );
            task.startThreadResourceTracking(
                i,
                ResourceStatsType.WORKER_STATS,
                new ResourceUsageMetric(ResourceStats.MEMORY, 0L),
                new ResourceUsageMetric(ResourceStats.CPU, 0L)
            );
            task.updateThreadResourceStats(
                i,
                ResourceStatsType.WORKER_STATS,
                new ResourceUsageMetric(ResourceStats.MEMORY, i * 100L),
                new ResourceUsageMetric(ResourceStats.CPU, i * 100L)
            );
            topNSearchTasksLogger.accept(task);
        }
    }
}
