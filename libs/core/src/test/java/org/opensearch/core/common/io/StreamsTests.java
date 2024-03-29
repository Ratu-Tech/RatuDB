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

package org.opensearch.core.common.io;

import org.apache.lucene.util.BytesRef;
import org.opensearch.common.io.Streams;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.opensearch.common.io.Streams.copyToString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Unit tests for {@link Streams}.
 */
public class StreamsTests extends OpenSearchTestCase {

    public void testCopyFromByteArray() throws IOException {
        byte[] content = "content".getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream out = new ByteArrayOutputStream(content.length);
        Streams.copy(content, out);
        assertThat(Arrays.equals(content, out.toByteArray()), equalTo(true));
    }

    public void testCopyFromReader() throws IOException {
        String content = "content";
        StringReader in = new StringReader(content);
        StringWriter out = new StringWriter();
        int count = Streams.copy(in, out);
        assertThat(content.length(), equalTo(count));
        assertThat(out.toString(), equalTo(content));
    }

    public void testCopyFromString() throws IOException {
        String content = "content";
        StringWriter out = new StringWriter();
        Streams.copy(content, out);
        assertThat(out.toString(), equalTo(content));
    }

    public void testCopyToString() throws IOException {
        String content = "content";
        StringReader in = new StringReader(content);
        String result = copyToString(in);
        assertThat(result, equalTo(content));
    }

    public void testBytesStreamInput() throws IOException {
        byte stuff[] = new byte[] { 0, 1, 2, 3 };
        BytesRef stuffRef = new BytesRef(stuff, 2, 2);
        BytesArray stuffArray = new BytesArray(stuffRef);
        StreamInput input = stuffArray.streamInput();
        assertEquals(2, input.read());
        assertEquals(3, input.read());
        assertEquals(-1, input.read());
        input.close();
    }

    public void testFullyConsumeInputStream() throws IOException {
        final String bytes = randomAlphaOfLengthBetween(0, 100);
        final BytesArray stuffArray = new BytesArray(bytes);
        assertEquals(bytes.length(), Streams.consumeFully(stuffArray.streamInput()));
    }

    public void testLimitInputStream() throws IOException {
        final byte[] bytes = randomAlphaOfLengthBetween(1, 100).getBytes(StandardCharsets.UTF_8);
        final int limit = randomIntBetween(0, bytes.length);
        final BytesArray stuffArray = new BytesArray(bytes);
        final ByteArrayOutputStream out = new ByteArrayOutputStream(bytes.length);
        final long count = org.opensearch.common.util.io.Streams.copy(Streams.limitStream(stuffArray.streamInput(), limit), out);
        assertEquals(limit, count);
        assertThat(Arrays.equals(out.toByteArray(), Arrays.copyOf(bytes, limit)), equalTo(true));
    }
}
