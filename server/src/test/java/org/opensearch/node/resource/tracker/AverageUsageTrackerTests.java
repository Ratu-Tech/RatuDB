/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node.resource.tracker;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.TimeUnit;

/**
 * Tests to validate AverageMemoryUsageTracker and AverageCpuUsageTracker implementation
 */
public class AverageUsageTrackerTests extends OpenSearchTestCase {
    ThreadPool threadPool;
    AverageMemoryUsageTracker averageMemoryUsageTracker;
    AverageCpuUsageTracker averageCpuUsageTracker;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(getClass().getName());
        averageMemoryUsageTracker = new AverageMemoryUsageTracker(
            threadPool,
            new TimeValue(500, TimeUnit.MILLISECONDS),
            new TimeValue(1000, TimeUnit.MILLISECONDS)
        );
        averageCpuUsageTracker = new AverageCpuUsageTracker(
            threadPool,
            new TimeValue(500, TimeUnit.MILLISECONDS),
            new TimeValue(1000, TimeUnit.MILLISECONDS)
        );
    }

    @After
    public void cleanup() {
        ThreadPool.terminate(threadPool, 5, TimeUnit.SECONDS);
    }

    public void testBasicUsage() {

        assertAverageUsageStats(averageMemoryUsageTracker);
        assertAverageUsageStats(averageCpuUsageTracker);
    }

    public void testUpdateWindowSize() {
        assertUpdateWindowSize(averageMemoryUsageTracker);
        assertUpdateWindowSize(averageCpuUsageTracker);
    }

    private void assertAverageUsageStats(AbstractAverageUsageTracker usageTracker) {
        usageTracker.recordUsage(1);
        assertFalse(usageTracker.isReady());
        usageTracker.recordUsage(2);
        assertTrue(usageTracker.isReady());
        assertEquals(2, usageTracker.getWindowSize());
        assertEquals(1.5, usageTracker.getAverage(), 0.0);
        usageTracker.recordUsage(5);
        // ( 2 + 5 ) / 2 = 3.5
        assertEquals(3.5, usageTracker.getAverage(), 0.0);
    }

    private void assertUpdateWindowSize(AbstractAverageUsageTracker usageTracker) {
        usageTracker.recordUsage(1);
        usageTracker.recordUsage(2);

        assertEquals(2, usageTracker.getWindowSize());
        assertEquals(1.5, usageTracker.getAverage(), 0.0);
        usageTracker.recordUsage(5);
        // ( 2 + 5 ) / 2 = 3.5
        assertEquals(3.5, usageTracker.getAverage(), 0.0);

        usageTracker.setWindowSize(new TimeValue(2000, TimeUnit.MILLISECONDS));
        assertEquals(0, usageTracker.getWindowSize());
        assertEquals(0.0, usageTracker.getAverage(), 0.0);
        // verify 2000/500 = 4 is the window size and average is calculated on window size of 4
        usageTracker.recordUsage(1);
        usageTracker.recordUsage(2);
        usageTracker.recordUsage(1);
        assertFalse(usageTracker.isReady());
        usageTracker.recordUsage(2);
        assertTrue(usageTracker.isReady());
        assertEquals(4, usageTracker.getWindowSize());
        // (1 + 2 + 1 + 2 ) / 4 = 1.5
        assertEquals(1.5, usageTracker.getAverage(), 0.0);
        usageTracker.recordUsage(2);
        assertTrue(usageTracker.isReady());
        // ( 2 + 1 + 2 + 2 ) / 4 = 1.75
        assertEquals(1.75, usageTracker.getAverage(), 0.0);
    }
}
