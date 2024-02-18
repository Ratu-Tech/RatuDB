/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.util;

import org.opensearch.common.settings.Settings;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LongHashTests extends OpenSearchTestCase {
    LongHash hash;

    private BigArrays randombigArrays() {
        return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

    private void newHash() {
        if (hash != null) {
            hash.close();
        }

        // Test high load factors to make sure that collision resolution works fine
        final float maxLoadFactor = 0.6f + randomFloat() * 0.39f;
        hash = new LongHash(randomIntBetween(0, 100), maxLoadFactor, randombigArrays());
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        newHash();
    }

    public void testDuell() {
        final Long[] values = new Long[randomIntBetween(1, 100000)];
        for (int i = 0; i < values.length; ++i) {
            values[i] = randomLong();
        }
        final Map<Long, Integer> valueToId = new HashMap<>();
        final long[] idToValue = new long[values.length];
        final int iters = randomInt(1000000);
        for (int i = 0; i < iters; ++i) {
            final Long value = randomFrom(values);
            if (valueToId.containsKey(value)) {
                assertEquals(-1 - valueToId.get(value), hash.add(value));
            } else {
                assertEquals(valueToId.size(), hash.add(value));
                idToValue[valueToId.size()] = value;
                valueToId.put(value, valueToId.size());
            }
        }

        assertEquals(valueToId.size(), hash.size());
        for (var iterator : valueToId.entrySet()) {
            assertEquals(iterator.getValue().longValue(), hash.find(iterator.getKey()));
        }

        for (long i = 0; i < hash.capacity(); ++i) {
            final long id = hash.id(i);
            if (id >= 0) {
                assertEquals(idToValue[(int) id], hash.get(id));
            }
        }

        for (long i = 0; i < hash.size(); i++) {
            assertEquals(idToValue[(int) i], hash.get(i));
        }

        hash.close();
    }

    public void testSize() {
        int num = scaledRandomIntBetween(2, 20);
        for (int j = 0; j < num; j++) {
            final int mod = 1 + randomInt(40);
            for (int i = 0; i < 797; i++) {
                long count = hash.size();
                long key = hash.add(randomLong());
                if (key < 0) assertEquals(hash.size(), count);
                else assertEquals(hash.size(), count + 1);
                if (i % mod == 0) {
                    newHash();
                }
            }
        }
        hash.close();
    }

    public void testKey() {
        int num = scaledRandomIntBetween(2, 20);
        for (int j = 0; j < num; j++) {
            Map<Long, Long> longs = new HashMap<>();
            int uniqueCount = 0;
            for (int i = 0; i < 797; i++) {
                long ref = randomLong();
                long count = hash.size();
                long key = hash.add(ref);
                if (key >= 0) {
                    assertNull(longs.put(ref, key));
                    assertEquals(uniqueCount, key);
                    uniqueCount++;
                    assertEquals(hash.size(), count + 1);
                } else {
                    assertTrue((-key) - 1L < count);
                    assertEquals(hash.size(), count);
                }
            }

            for (Map.Entry<Long, Long> entry : longs.entrySet()) {
                long expected = entry.getKey();
                long keyIdx = entry.getValue();
                assertEquals(expected, hash.get(keyIdx));
            }

            newHash();
        }
        hash.close();
    }

    public void testAdd() {
        int num = scaledRandomIntBetween(2, 20);
        for (int j = 0; j < num; j++) {
            Set<Long> longs = new HashSet<>();
            int uniqueCount = 0;
            for (int i = 0; i < 797; i++) {
                long ref = randomLong();
                long count = hash.size();
                long key = hash.add(ref);
                if (key >= 0) {
                    assertTrue(longs.add(ref));
                    assertEquals(uniqueCount, key);
                    assertEquals(hash.size(), count + 1);
                    uniqueCount++;
                } else {
                    assertFalse(longs.add(ref));
                    assertTrue((-key) - 1 < count);
                    assertEquals(ref, hash.get((-key) - 1));
                    assertEquals(count, hash.size());
                }
            }

            assertAllIn(longs, hash);
            newHash();
        }
        hash.close();
    }

    public void testFind() throws Exception {
        int num = scaledRandomIntBetween(2, 20);
        for (int j = 0; j < num; j++) {
            Set<Long> longs = new HashSet<>();
            int uniqueCount = 0;
            for (int i = 0; i < 797; i++) {
                long ref = randomLong();
                long count = hash.size();
                long key = hash.find(ref);
                if (key >= 0) { // found in hash
                    assertFalse(longs.add(ref));
                    assertTrue(key < count);
                    assertEquals(ref, hash.get(key));
                    assertEquals(count, hash.size());
                } else {
                    key = hash.add(ref);
                    assertTrue(longs.add(ref));
                    assertEquals(uniqueCount, key);
                    assertEquals(hash.size(), count + 1);
                    uniqueCount++;
                }
            }

            assertAllIn(longs, hash);
            newHash();
        }
        hash.close();
    }

    private static void assertAllIn(Set<Long> longs, LongHash hash) {
        long count = hash.size();
        for (Long l : longs) {
            long key = hash.add(l); // add again to check duplicates
            assertEquals(l.longValue(), hash.get((-key) - 1));
            assertEquals(count, hash.size());
            assertTrue("key: " + key + " count: " + count + " long: " + l, key < count);
        }
    }
}
