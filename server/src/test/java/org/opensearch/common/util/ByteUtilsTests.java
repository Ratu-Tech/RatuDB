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

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class ByteUtilsTests extends OpenSearchTestCase {

    public void testZigZag(long l) {
        assertEquals(l, ByteUtils.zigZagDecode(ByteUtils.zigZagEncode(l)));
    }

    public void testZigZag() {
        testZigZag(0);
        testZigZag(1);
        testZigZag(-1);
        testZigZag(Long.MAX_VALUE);
        testZigZag(Long.MIN_VALUE);
        for (int i = 0; i < 1000; ++i) {
            testZigZag(randomLong());
            assertTrue(ByteUtils.zigZagEncode(randomInt(1000)) >= 0);
            assertTrue(ByteUtils.zigZagEncode(-randomInt(1000)) >= 0);
        }
    }

    public void testFloat() throws IOException {
        final float[] data = new float[scaledRandomIntBetween(1000, 10000)];
        final byte[] encoded = new byte[data.length * 4];
        for (int i = 0; i < data.length; ++i) {
            data[i] = randomFloat();
            ByteUtils.writeFloatLE(data[i], encoded, i * 4);
        }
        for (int i = 0; i < data.length; ++i) {
            assertEquals(data[i], ByteUtils.readFloatLE(encoded, i * 4), Float.MIN_VALUE);
        }
    }

    public void testDouble() throws IOException {
        final double[] data = new double[scaledRandomIntBetween(1000, 10000)];
        final byte[] encoded = new byte[data.length * 8];
        for (int i = 0; i < data.length; ++i) {
            data[i] = randomDouble();
            ByteUtils.writeDoubleLE(data[i], encoded, i * 8);
        }
        for (int i = 0; i < data.length; ++i) {
            assertEquals(data[i], ByteUtils.readDoubleLE(encoded, i * 8), Double.MIN_VALUE);
        }
    }

}
