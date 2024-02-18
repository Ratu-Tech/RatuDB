/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CloseableRetryableRefreshListenerTests extends OpenSearchTestCase {

    private static final Logger logger = LogManager.getLogger(CloseableRetryableRefreshListenerTests.class);

    private ThreadPool threadPool;

    @Before
    public void init() {
        threadPool = new TestThreadPool(getTestName());
    }

    /**
     * This tests that the performAfterRefresh method is being invoked when the afterRefresh method is invoked. We check that the countDownLatch is decreasing as intended to validate that the performAfterRefresh is being invoked.
     */
    public void testPerformAfterRefresh() throws IOException {

        CountDownLatch countDownLatch = new CountDownLatch(2);
        CloseableRetryableRefreshListener testRefreshListener = new CloseableRetryableRefreshListener(mock(ThreadPool.class)) {
            @Override
            protected boolean performAfterRefreshWithPermit(boolean didRefresh) {
                countDownLatch.countDown();
                return false;
            }

            @Override
            public void beforeRefresh() {}

            @Override
            protected Logger getLogger() {
                return logger;
            }
        };

        // First invocation of afterRefresh method
        testRefreshListener.afterRefresh(true);
        assertEquals(1, countDownLatch.getCount());

        // Second invocation of afterRefresh method
        testRefreshListener.afterRefresh(true);
        assertEquals(0, countDownLatch.getCount());
        testRefreshListener.close();
    }

    /**
     * This tests that close is acquiring all permits and even if the afterRefresh method is called, it is no-op.
     */
    public void testCloseAfterRefresh() throws IOException {
        final int initialCount = randomIntBetween(10, 100);
        final CountDownLatch countDownLatch = new CountDownLatch(initialCount);
        CloseableRetryableRefreshListener testRefreshListener = new CloseableRetryableRefreshListener(mock(ThreadPool.class)) {
            @Override
            protected boolean performAfterRefreshWithPermit(boolean didRefresh) {
                countDownLatch.countDown();
                return false;
            }

            @Override
            public void beforeRefresh() {}

            @Override
            protected Logger getLogger() {
                return logger;
            }
        };

        int refreshCount = randomIntBetween(1, initialCount);
        for (int i = 0; i < refreshCount; i++) {
            testRefreshListener.afterRefresh(true);
        }
        assertEquals(initialCount - refreshCount, countDownLatch.getCount());

        // Closing the refresh listener so that no further afterRefreshes are executed going forward
        testRefreshListener.close();

        for (int i = 0; i < initialCount - refreshCount; i++) {
            testRefreshListener.afterRefresh(true);
        }
        assertEquals(initialCount - refreshCount, countDownLatch.getCount());
    }

    /**
     * This tests that the retry does not get triggered when there are missing configurations or method overrides that empowers the retry to happen.
     */
    public void testNoRetry() throws IOException {
        int initialCount = randomIntBetween(10, 100);
        final CountDownLatch countDownLatch = new CountDownLatch(initialCount);
        CloseableRetryableRefreshListener testRefreshListener = new CloseableRetryableRefreshListener(mock(ThreadPool.class)) {
            @Override
            protected boolean performAfterRefreshWithPermit(boolean didRefresh) {
                countDownLatch.countDown();
                return countDownLatch.getCount() == 0;
            }

            @Override
            public void beforeRefresh() {}

            @Override
            protected Logger getLogger() {
                return logger;
            }
        };
        testRefreshListener.afterRefresh(true);
        assertEquals(initialCount - 1, countDownLatch.getCount());
        testRefreshListener.close();

        testRefreshListener = new CloseableRetryableRefreshListener(threadPool) {
            @Override
            protected boolean performAfterRefreshWithPermit(boolean didRefresh) {
                countDownLatch.countDown();
                return countDownLatch.getCount() == 0;
            }

            @Override
            public void beforeRefresh() {}

            @Override
            protected Logger getLogger() {
                return logger;
            }
        };
        testRefreshListener.afterRefresh(true);
        assertEquals(initialCount - 2, countDownLatch.getCount());
        testRefreshListener.close();

        testRefreshListener = new CloseableRetryableRefreshListener(threadPool) {
            @Override
            protected boolean performAfterRefreshWithPermit(boolean didRefresh) {
                countDownLatch.countDown();
                return countDownLatch.getCount() == 0;
            }

            @Override
            public void beforeRefresh() {}

            @Override
            protected String getRetryThreadPoolName() {
                return ThreadPool.Names.REMOTE_REFRESH_RETRY;
            }

            @Override
            protected Logger getLogger() {
                return logger;
            }
        };
        testRefreshListener.afterRefresh(true);
        assertEquals(initialCount - 3, countDownLatch.getCount());
        testRefreshListener.close();

        testRefreshListener = new CloseableRetryableRefreshListener(threadPool) {
            @Override
            protected boolean performAfterRefreshWithPermit(boolean didRefresh) {
                countDownLatch.countDown();
                return countDownLatch.getCount() == 0;
            }

            @Override
            public void beforeRefresh() {}

            @Override
            protected TimeValue getNextRetryInterval() {
                return TimeValue.timeValueMillis(100);
            }

            @Override
            protected Logger getLogger() {
                return logger;
            }
        };
        testRefreshListener.afterRefresh(true);
        assertEquals(initialCount - 4, countDownLatch.getCount());
        testRefreshListener.close();
    }

    /**
     * This tests that retry gets scheduled and executed when the configurations and method overrides are done properly.
     */
    public void testRetry() throws Exception {
        int initialCount = randomIntBetween(10, 20);
        final CountDownLatch countDownLatch = new CountDownLatch(initialCount);
        CloseableRetryableRefreshListener testRefreshListener = new CloseableRetryableRefreshListener(threadPool) {
            @Override
            protected boolean performAfterRefreshWithPermit(boolean didRefresh) {
                countDownLatch.countDown();
                return countDownLatch.getCount() == 0;
            }

            @Override
            public void beforeRefresh() {}

            @Override
            protected String getRetryThreadPoolName() {
                return ThreadPool.Names.REMOTE_REFRESH_RETRY;
            }

            @Override
            protected TimeValue getNextRetryInterval() {
                return TimeValue.timeValueMillis(100);
            }

            @Override
            protected Logger getLogger() {
                return logger;
            }

            @Override
            protected boolean isRetryEnabled() {
                return true;
            }
        };
        testRefreshListener.afterRefresh(true);
        assertBusy(() -> assertEquals(0, countDownLatch.getCount()));
        testRefreshListener.close();
    }

    /**
     * This tests that once close method is invoked, then even the retries would become no-op.
     */
    public void testCloseWithRetryPending() throws IOException {
        int initialCount = randomIntBetween(10, 20);
        final CountDownLatch countDownLatch = new CountDownLatch(initialCount);
        CloseableRetryableRefreshListener testRefreshListener = new CloseableRetryableRefreshListener(threadPool) {
            @Override
            protected boolean performAfterRefreshWithPermit(boolean didRefresh) {
                countDownLatch.countDown();
                return countDownLatch.getCount() == 0;
            }

            @Override
            public void beforeRefresh() {}

            @Override
            protected String getRetryThreadPoolName() {
                return ThreadPool.Names.REMOTE_REFRESH_RETRY;
            }

            @Override
            protected TimeValue getNextRetryInterval() {
                return TimeValue.timeValueMillis(100);
            }

            @Override
            protected Logger getLogger() {
                return logger;
            }
        };
        testRefreshListener.afterRefresh(randomBoolean());
        testRefreshListener.close();
        assertNotEquals(0, countDownLatch.getCount());
    }

    public void testCloseWaitsForAcquiringAllPermits() throws Exception {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        CloseableRetryableRefreshListener testRefreshListener = new CloseableRetryableRefreshListener(threadPool) {
            @Override
            protected boolean performAfterRefreshWithPermit(boolean didRefresh) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                countDownLatch.countDown();
                return false;
            }

            @Override
            public void beforeRefresh() {}

            @Override
            protected Logger getLogger() {
                return logger;
            }
        };
        Thread thread = new Thread(() -> {
            try {
                testRefreshListener.afterRefresh(randomBoolean());
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
        thread.start();
        assertBusy(() -> assertEquals(0, countDownLatch.getCount()));
        testRefreshListener.close();
    }

    public void testScheduleRetryAfterClose() throws Exception {
        // This tests that once the listener has been closed, even the retries would not be scheduled.
        final AtomicLong runCount = new AtomicLong();
        CloseableRetryableRefreshListener testRefreshListener = new CloseableRetryableRefreshListener(threadPool) {
            @Override
            protected boolean performAfterRefreshWithPermit(boolean didRefresh) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                runCount.incrementAndGet();
                return false;
            }

            @Override
            public void beforeRefresh() {}

            @Override
            protected Logger getLogger() {
                return logger;
            }

            @Override
            protected String getRetryThreadPoolName() {
                return ThreadPool.Names.REMOTE_REFRESH_RETRY;
            }

            @Override
            protected TimeValue getNextRetryInterval() {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                return TimeValue.timeValueMillis(100);
            }
        };
        Thread thread1 = new Thread(() -> {
            try {
                testRefreshListener.afterRefresh(true);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
        Thread thread2 = new Thread(() -> {
            try {
                Thread.sleep(500);
                testRefreshListener.close();
            } catch (IOException | InterruptedException e) {
                throw new AssertionError(e);
            }
        });
        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();
        assertBusy(() -> assertEquals(1, runCount.get()));
    }

    public void testConcurrentScheduleRetry() throws Exception {
        // This tests that there can be only 1 retry that can be scheduled at a time.
        final AtomicLong runCount = new AtomicLong();
        final AtomicInteger retryCount = new AtomicInteger(0);
        CloseableRetryableRefreshListener testRefreshListener = new CloseableRetryableRefreshListener(threadPool) {
            @Override
            protected boolean performAfterRefreshWithPermit(boolean didRefresh) {
                retryCount.incrementAndGet();
                runCount.incrementAndGet();
                return retryCount.get() >= 2;
            }

            @Override
            public void beforeRefresh() {}

            @Override
            protected Logger getLogger() {
                return logger;
            }

            @Override
            protected String getRetryThreadPoolName() {
                return ThreadPool.Names.REMOTE_REFRESH_RETRY;
            }

            @Override
            protected TimeValue getNextRetryInterval() {
                return TimeValue.timeValueMillis(5000);
            }

            @Override
            protected boolean isRetryEnabled() {
                return true;
            }
        };
        testRefreshListener.afterRefresh(true);
        testRefreshListener.afterRefresh(true);
        assertBusy(() -> assertEquals(3, runCount.get()));
        testRefreshListener.close();
    }

    public void testExceptionDuringThreadPoolSchedule() throws Exception {
        // This tests that if there are exceptions while scheduling the task in the threadpool, the retrySchedule boolean
        // is reset properly to allow future scheduling to happen.
        AtomicInteger runCount = new AtomicInteger();
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        when(mockThreadPool.schedule(any(), any(), any())).thenThrow(new RuntimeException());
        CloseableRetryableRefreshListener testRefreshListener = new CloseableRetryableRefreshListener(mockThreadPool) {
            @Override
            protected boolean performAfterRefreshWithPermit(boolean didRefresh) {
                runCount.incrementAndGet();
                return false;
            }

            @Override
            public void beforeRefresh() {}

            @Override
            protected Logger getLogger() {
                return logger;
            }

            @Override
            protected String getRetryThreadPoolName() {
                return ThreadPool.Names.REMOTE_REFRESH_RETRY;
            }

            @Override
            protected TimeValue getNextRetryInterval() {
                return TimeValue.timeValueMillis(100);
            }

            @Override
            protected boolean isRetryEnabled() {
                return true;
            }
        };
        assertThrows(RuntimeException.class, () -> testRefreshListener.afterRefresh(true));
        assertBusy(() -> assertFalse(testRefreshListener.getRetryScheduledStatus()));
        assertEquals(1, runCount.get());
        testRefreshListener.close();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }
}
