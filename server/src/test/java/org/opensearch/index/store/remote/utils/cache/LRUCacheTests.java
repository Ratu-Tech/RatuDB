/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils.cache;

public class LRUCacheTests extends RefCountedCacheTestCase {
    public LRUCacheTests() {
        super(new LRUCache<>(CAPACITY, n -> {}, value -> value));
    }
}
