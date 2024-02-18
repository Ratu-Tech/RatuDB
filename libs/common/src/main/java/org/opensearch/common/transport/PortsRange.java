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

package org.opensearch.common.transport;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Port range utility classes
 *
 * @opensearch.internal
 */
public class PortsRange {

    private final String portRange;

    public PortsRange(String portRange) {
        this.portRange = portRange;
    }

    public String getPortRangeString() {
        return portRange;
    }

    public int[] ports() throws NumberFormatException {
        final List<Integer> ports = new ArrayList<>();
        iterate(portNumber -> {
            ports.add(portNumber);
            return false;
        });
        return ports.stream().mapToInt(Integer::intValue).toArray();
    }

    public boolean iterate(PortCallback callback) throws NumberFormatException {
        StringTokenizer st = new StringTokenizer(portRange, ",");
        boolean success = false;
        while (st.hasMoreTokens() && !success) {
            String portToken = st.nextToken().trim();
            int index = portToken.indexOf('-');
            if (index == -1) {
                int portNumber = Integer.parseInt(portToken.trim());
                success = callback.onPortNumber(portNumber);
                if (success) {
                    break;
                }
            } else {
                int startPort = Integer.parseInt(portToken.substring(0, index).trim());
                int endPort = Integer.parseInt(portToken.substring(index + 1).trim());
                if (endPort < startPort) {
                    throw new IllegalArgumentException("Start port [" + startPort + "] must be greater than end port [" + endPort + "]");
                }
                for (int i = startPort; i <= endPort; i++) {
                    success = callback.onPortNumber(i);
                    if (success) {
                        break;
                    }
                }
            }
        }
        return success;
    }

    /**
     * Callback for the port
     *
     * @opensearch.internal
     */
    public interface PortCallback {
        boolean onPortNumber(int portNumber);
    }

    @Override
    public String toString() {
        return "PortsRange{" + "portRange='" + portRange + '\'' + '}';
    }
}
