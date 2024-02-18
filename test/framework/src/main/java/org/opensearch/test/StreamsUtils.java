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

package org.opensearch.test;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.util.io.Streams;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class StreamsUtils {

    public static String copyToStringFromClasspath(ClassLoader classLoader, String path) throws IOException {
        InputStream is = classLoader.getResourceAsStream(path);
        if (is == null) {
            throw new FileNotFoundException("Resource [" + path + "] not found in classpath with class loader [" + classLoader + "]");
        }
        return org.opensearch.common.io.Streams.copyToString(new InputStreamReader(is, StandardCharsets.UTF_8));
    }

    public static String copyToStringFromClasspath(String path) throws IOException {
        InputStream is = Streams.class.getResourceAsStream(path);
        if (is == null) {
            throw new FileNotFoundException("Resource [" + path + "] not found in classpath");
        }
        return org.opensearch.common.io.Streams.copyToString(new InputStreamReader(is, StandardCharsets.UTF_8));
    }

    public static byte[] copyToBytesFromClasspath(String path) throws IOException {
        try (InputStream is = Streams.class.getResourceAsStream(path)) {
            if (is == null) {
                throw new FileNotFoundException("Resource [" + path + "] not found in classpath");
            }
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                Streams.copy(is, out);
                return BytesReference.toBytes(out.bytes());
            }
        }
    }

}
