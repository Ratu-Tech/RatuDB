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

package org.opensearch.action.admin.cluster.node.info;

import org.opensearch.Build;
import org.opensearch.Version;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Nullable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.service.ReportingService;
import org.opensearch.http.HttpInfo;
import org.opensearch.ingest.IngestInfo;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.monitor.os.OsInfo;
import org.opensearch.monitor.process.ProcessInfo;
import org.opensearch.search.aggregations.support.AggregationInfo;
import org.opensearch.search.pipeline.SearchPipelineInfo;
import org.opensearch.threadpool.ThreadPoolInfo;
import org.opensearch.transport.TransportInfo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Node information (static, does not change over time).
 *
 * @opensearch.internal
 */
public class NodeInfo extends BaseNodeResponse {

    private Version version;
    private Build build;

    @Nullable
    private Settings settings;

    /**
     * Do not expose this map to other classes. For type safety, use {@link #getInfo(Class)}
     * to retrieve items from this map and {@link #addInfoIfNonNull(Class, ReportingService.Info)}
     * to retrieve items from it.
     */
    private Map<Class<? extends ReportingService.Info>, ReportingService.Info> infoMap = new HashMap<>();

    @Nullable
    private ByteSizeValue totalIndexingBuffer;

    public NodeInfo(StreamInput in) throws IOException {
        super(in);
        version = in.readVersion();
        build = in.readBuild();
        if (in.readBoolean()) {
            totalIndexingBuffer = new ByteSizeValue(in.readLong());
        } else {
            totalIndexingBuffer = null;
        }
        if (in.readBoolean()) {
            settings = Settings.readSettingsFromStream(in);
        }
        addInfoIfNonNull(OsInfo.class, in.readOptionalWriteable(OsInfo::new));
        addInfoIfNonNull(ProcessInfo.class, in.readOptionalWriteable(ProcessInfo::new));
        addInfoIfNonNull(JvmInfo.class, in.readOptionalWriteable(JvmInfo::new));
        addInfoIfNonNull(ThreadPoolInfo.class, in.readOptionalWriteable(ThreadPoolInfo::new));
        addInfoIfNonNull(TransportInfo.class, in.readOptionalWriteable(TransportInfo::new));
        addInfoIfNonNull(HttpInfo.class, in.readOptionalWriteable(HttpInfo::new));
        addInfoIfNonNull(PluginsAndModules.class, in.readOptionalWriteable(PluginsAndModules::new));
        addInfoIfNonNull(IngestInfo.class, in.readOptionalWriteable(IngestInfo::new));
        addInfoIfNonNull(AggregationInfo.class, in.readOptionalWriteable(AggregationInfo::new));
        if (in.getVersion().onOrAfter(Version.V_2_7_0)) {
            addInfoIfNonNull(SearchPipelineInfo.class, in.readOptionalWriteable(SearchPipelineInfo::new));
        }
    }

    public NodeInfo(
        Version version,
        Build build,
        DiscoveryNode node,
        @Nullable Settings settings,
        @Nullable OsInfo os,
        @Nullable ProcessInfo process,
        @Nullable JvmInfo jvm,
        @Nullable ThreadPoolInfo threadPool,
        @Nullable TransportInfo transport,
        @Nullable HttpInfo http,
        @Nullable PluginsAndModules plugins,
        @Nullable IngestInfo ingest,
        @Nullable AggregationInfo aggsInfo,
        @Nullable ByteSizeValue totalIndexingBuffer,
        @Nullable SearchPipelineInfo searchPipelineInfo
    ) {
        super(node);
        this.version = version;
        this.build = build;
        this.settings = settings;
        addInfoIfNonNull(OsInfo.class, os);
        addInfoIfNonNull(ProcessInfo.class, process);
        addInfoIfNonNull(JvmInfo.class, jvm);
        addInfoIfNonNull(ThreadPoolInfo.class, threadPool);
        addInfoIfNonNull(TransportInfo.class, transport);
        addInfoIfNonNull(HttpInfo.class, http);
        addInfoIfNonNull(PluginsAndModules.class, plugins);
        addInfoIfNonNull(IngestInfo.class, ingest);
        addInfoIfNonNull(AggregationInfo.class, aggsInfo);
        addInfoIfNonNull(SearchPipelineInfo.class, searchPipelineInfo);
        this.totalIndexingBuffer = totalIndexingBuffer;
    }

    /**
     * System's hostname. <code>null</code> in case of UnknownHostException
     */
    @Nullable
    public String getHostname() {
        return getNode().getHostName();
    }

    /**
     * The current OpenSearch version
     */
    public Version getVersion() {
        return version;
    }

    /**
     * The build version of the node.
     */
    public Build getBuild() {
        return this.build;
    }

    /**
     * The settings of the node.
     */
    @Nullable
    public Settings getSettings() {
        return this.settings;
    }

    /**
     * Get a particular info object, e.g. {@link JvmInfo} or {@link OsInfo}. This
     * generic method handles all casting in order to spare client classes the
     * work of explicit casts. This {@link NodeInfo} class guarantees type
     * safety for these stored info blocks.
     *
     * @param clazz Class for retrieval.
     * @param <T>   Specific subtype of ReportingService.Info to retrieve.
     * @return      An object of type T.
     */
    public <T extends ReportingService.Info> T getInfo(Class<T> clazz) {
        return clazz.cast(infoMap.get(clazz));
    }

    @Nullable
    public ByteSizeValue getTotalIndexingBuffer() {
        return totalIndexingBuffer;
    }

    /**
     * Add a value to the map of information blocks. This method guarantees the
     * type safety of the storage of heterogeneous types of reporting service information.
     */
    private <T extends ReportingService.Info> void addInfoIfNonNull(Class<T> clazz, T info) {
        if (info != null) {
            infoMap.put(clazz, info);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(version.id);
        out.writeBuild(build);
        if (totalIndexingBuffer == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeLong(totalIndexingBuffer.getBytes());
        }
        if (settings == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Settings.writeSettingsToStream(settings, out);
        }
        out.writeOptionalWriteable(getInfo(OsInfo.class));
        out.writeOptionalWriteable(getInfo(ProcessInfo.class));
        out.writeOptionalWriteable(getInfo(JvmInfo.class));
        out.writeOptionalWriteable(getInfo(ThreadPoolInfo.class));
        out.writeOptionalWriteable(getInfo(TransportInfo.class));
        out.writeOptionalWriteable(getInfo(HttpInfo.class));
        out.writeOptionalWriteable(getInfo(PluginsAndModules.class));
        out.writeOptionalWriteable(getInfo(IngestInfo.class));
        out.writeOptionalWriteable(getInfo(AggregationInfo.class));
        if (out.getVersion().onOrAfter(Version.V_2_7_0)) {
            out.writeOptionalWriteable(getInfo(SearchPipelineInfo.class));
        }
    }

    public static NodeInfo.Builder builder(Version version, Build build, DiscoveryNode node) {
        return new Builder(version, build, node);
    }

    /**
     * Builder class to accommodate new Info types being added to NodeInfo.
     */
    public static class Builder {
        private final Version version;
        private final Build build;
        private final DiscoveryNode node;

        private Builder(Version version, Build build, DiscoveryNode node) {
            this.version = version;
            this.build = build;
            this.node = node;
        }

        private Settings settings;
        private OsInfo os;
        private ProcessInfo process;
        private JvmInfo jvm;
        private ThreadPoolInfo threadPool;
        private TransportInfo transport;
        private HttpInfo http;
        private PluginsAndModules plugins;
        private IngestInfo ingest;
        private AggregationInfo aggsInfo;
        private ByteSizeValue totalIndexingBuffer;
        private SearchPipelineInfo searchPipelineInfo;

        public Builder setSettings(Settings settings) {
            this.settings = settings;
            return this;
        }

        public Builder setOs(OsInfo os) {
            this.os = os;
            return this;
        }

        public Builder setProcess(ProcessInfo process) {
            this.process = process;
            return this;
        }

        public Builder setJvm(JvmInfo jvm) {
            this.jvm = jvm;
            return this;
        }

        public Builder setThreadPool(ThreadPoolInfo threadPool) {
            this.threadPool = threadPool;
            return this;
        }

        public Builder setTransport(TransportInfo transport) {
            this.transport = transport;
            return this;
        }

        public Builder setHttp(HttpInfo http) {
            this.http = http;
            return this;
        }

        public Builder setPlugins(PluginsAndModules plugins) {
            this.plugins = plugins;
            return this;
        }

        public Builder setIngest(IngestInfo ingest) {
            this.ingest = ingest;
            return this;
        }

        public Builder setAggsInfo(AggregationInfo aggsInfo) {
            this.aggsInfo = aggsInfo;
            return this;
        }

        public Builder setTotalIndexingBuffer(ByteSizeValue totalIndexingBuffer) {
            this.totalIndexingBuffer = totalIndexingBuffer;
            return this;
        }

        public Builder setSearchPipelineInfo(SearchPipelineInfo searchPipelineInfo) {
            this.searchPipelineInfo = searchPipelineInfo;
            return this;
        }

        public NodeInfo build() {
            return new NodeInfo(
                version,
                build,
                node,
                settings,
                os,
                process,
                jvm,
                threadPool,
                transport,
                http,
                plugins,
                ingest,
                aggsInfo,
                totalIndexingBuffer,
                searchPipelineInfo
            );
        }

    }

}
