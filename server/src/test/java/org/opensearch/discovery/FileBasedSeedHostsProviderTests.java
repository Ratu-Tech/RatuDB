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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.discovery;

import org.opensearch.Version;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.nio.MockNioTransport;
import org.junit.After;
import org.junit.Before;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.opensearch.discovery.FileBasedSeedHostsProvider.UNICAST_HOSTS_FILE;

public class FileBasedSeedHostsProviderTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private ExecutorService executorService;
    private MockTransportService transportService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(FileBasedSeedHostsProviderTests.class.getName());
        executorService = Executors.newSingleThreadExecutor();
        createTransportSvc();
    }

    @After
    public void tearDown() throws Exception {
        try {
            terminate(executorService);
        } finally {
            try {
                terminate(threadPool);
            } finally {
                super.tearDown();
            }
        }
    }

    private void createTransportSvc() {
        final MockNioTransport transport = new MockNioTransport(
            Settings.EMPTY,
            Version.CURRENT,
            threadPool,
            new NetworkService(Collections.emptyList()),
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            new NamedWriteableRegistry(Collections.emptyList()),
            new NoneCircuitBreakerService(),
            NoopTracer.INSTANCE
        ) {
            @Override
            public BoundTransportAddress boundAddress() {
                return new BoundTransportAddress(
                    new TransportAddress[] { new TransportAddress(InetAddress.getLoopbackAddress(), 9300) },
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9300)
                );
            }
        };
        transportService = new MockTransportService(
            Settings.EMPTY,
            transport,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            null,
            NoopTracer.INSTANCE
        );
    }

    public void testBuildDynamicNodes() throws Exception {
        final List<String> hostEntries = Arrays.asList("#comment, should be ignored", "192.168.0.1", "192.168.0.2:9305", "255.255.23.15");
        final List<TransportAddress> nodes = setupAndRunHostProvider(hostEntries);
        assertEquals(hostEntries.size() - 1, nodes.size()); // minus 1 because we are ignoring the first line that's a comment
        assertEquals("192.168.0.1", nodes.get(0).getAddress());
        assertEquals(9300, nodes.get(0).getPort());
        assertEquals("192.168.0.2", nodes.get(1).getAddress());
        assertEquals(9305, nodes.get(1).getPort());
        assertEquals("255.255.23.15", nodes.get(2).getAddress());
        assertEquals(9300, nodes.get(2).getPort());
    }

    public void testEmptyUnicastHostsFile() throws Exception {
        final List<String> hostEntries = Collections.emptyList();
        final List<TransportAddress> addresses = setupAndRunHostProvider(hostEntries);
        assertEquals(0, addresses.size());
    }

    public void testUnicastHostsDoesNotExist() {
        final FileBasedSeedHostsProvider provider = new FileBasedSeedHostsProvider(createTempDir().toAbsolutePath());
        final List<TransportAddress> addresses = provider.getSeedAddresses(
            hosts -> SeedHostsResolver.resolveHostsLists(
                new CancellableThreads(),
                executorService,
                logger,
                hosts,
                transportService,
                TimeValue.timeValueSeconds(10)
            )
        );
        assertEquals(0, addresses.size());
    }

    public void testInvalidHostEntries() throws Exception {
        final List<String> hostEntries = Collections.singletonList("192.168.0.1:9300:9300");
        final List<TransportAddress> addresses = setupAndRunHostProvider(hostEntries);
        assertEquals(0, addresses.size());
    }

    public void testSomeInvalidHostEntries() throws Exception {
        final List<String> hostEntries = Arrays.asList("192.168.0.1:9300:9300", "192.168.0.1:9301");
        final List<TransportAddress> addresses = setupAndRunHostProvider(hostEntries);
        assertEquals(1, addresses.size()); // only one of the two is valid and will be used
        assertEquals("192.168.0.1", addresses.get(0).getAddress());
        assertEquals(9301, addresses.get(0).getPort());
    }

    // sets up the config dir, writes to the unicast hosts file in the config dir,
    // and then runs the file-based unicast host provider to get the list of discovery nodes
    private List<TransportAddress> setupAndRunHostProvider(final List<String> hostEntries) throws IOException {
        final Path homeDir = createTempDir();
        final Path configPath = randomBoolean() ? homeDir.resolve("config") : createTempDir();
        Files.createDirectories(configPath);
        try (BufferedWriter writer = Files.newBufferedWriter(configPath.resolve(UNICAST_HOSTS_FILE))) {
            writer.write(String.join("\n", hostEntries));
        }

        return new FileBasedSeedHostsProvider(configPath).getSeedAddresses(
            hosts -> SeedHostsResolver.resolveHostsLists(
                new CancellableThreads(),
                executorService,
                logger,
                hosts,
                transportService,
                TimeValue.timeValueSeconds(10)
            )
        );
    }
}
