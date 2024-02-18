/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils.cache;

/**
 * Usage metrics for {@link RefCountedCache}
 *
 * @opensearch.internal
 */
public class CacheUsage {
    /**
     * Cache usage of the system
     */
    private final long usage;

    /**
     * Cache usage by entries which are referenced
     */
    private final long activeUsage;

    public CacheUsage(long usage, long activeUsage) {
        this.usage = usage;
        this.activeUsage = activeUsage;
    }

    public long usage() {
        return usage;
    }

    public long activeUsage() {
        return activeUsage;
    }
}
