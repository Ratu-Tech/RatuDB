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

package org.opensearch.repositories.azure;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.repositories.blobstore.MeteredBlobStoreRepository;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import static org.opensearch.repositories.azure.AzureStorageService.MAX_CHUNK_SIZE;
import static org.opensearch.repositories.azure.AzureStorageService.MIN_CHUNK_SIZE;

/**
 * Azure file system implementation of the BlobStoreRepository
 * <p>
 * Azure file system repository supports the following settings:
 * <dl>
 * <dt>{@code container}</dt><dd>Azure container name. Defaults to opensearch-snapshots</dd>
 * <dt>{@code base_path}</dt><dd>Specifies the path within bucket to repository data. Defaults to root directory.</dd>
 * <dt>{@code chunk_size}</dt><dd>Large file can be divided into chunks. This parameter specifies the chunk size. Defaults to 64mb.</dd>
 * <dt>{@code compress}</dt><dd>If set to true metadata files will be stored compressed. Defaults to false.</dd>
 * </dl>
 */
public class AzureRepository extends MeteredBlobStoreRepository {
    private static final Logger logger = LogManager.getLogger(AzureRepository.class);

    public static final String TYPE = "azure";

    public static final class Repository {
        @Deprecated // Replaced by client
        public static final Setting<String> ACCOUNT_SETTING = new Setting<>(
            "account",
            "default",
            Function.identity(),
            Property.NodeScope,
            Property.Deprecated
        );
        public static final Setting<String> CLIENT_NAME = new Setting<>("client", ACCOUNT_SETTING, Function.identity());
        public static final Setting<String> CONTAINER_SETTING = new Setting<>(
            "container",
            "opensearch-snapshots",
            Function.identity(),
            Property.NodeScope
        );
        public static final Setting<String> BASE_PATH_SETTING = Setting.simpleString("base_path", Property.NodeScope);
        public static final Setting<LocationMode> LOCATION_MODE_SETTING = new Setting<>(
            "location_mode",
            s -> LocationMode.PRIMARY_ONLY.toString(),
            s -> LocationMode.valueOf(s.toUpperCase(Locale.ROOT)),
            Property.NodeScope
        );
        public static final Setting<ByteSizeValue> CHUNK_SIZE_SETTING = Setting.byteSizeSetting(
            "chunk_size",
            MAX_CHUNK_SIZE,
            MIN_CHUNK_SIZE,
            MAX_CHUNK_SIZE,
            Property.NodeScope
        );
    }

    private final BlobPath basePath;
    private final ByteSizeValue chunkSize;
    private final AzureStorageService storageService;
    private final boolean readonly;

    public AzureRepository(
        final RepositoryMetadata metadata,
        final NamedXContentRegistry namedXContentRegistry,
        final AzureStorageService storageService,
        final ClusterService clusterService,
        final RecoverySettings recoverySettings
    ) {
        super(metadata, namedXContentRegistry, clusterService, recoverySettings, buildLocation(metadata));
        this.chunkSize = Repository.CHUNK_SIZE_SETTING.get(metadata.settings());
        this.storageService = storageService;

        final String basePath = Strings.trimLeadingCharacter(Repository.BASE_PATH_SETTING.get(metadata.settings()), '/');
        if (Strings.hasLength(basePath)) {
            // Remove starting / if any
            BlobPath path = new BlobPath();
            for (final String elem : basePath.split("/")) {
                path = path.add(elem);
            }
            this.basePath = path;
        } else {
            this.basePath = BlobPath.cleanPath();
        }

        // If the user explicitly did not define a readonly value, we set it by ourselves depending on the location mode setting.
        // For secondary_only setting, the repository should be read only
        final LocationMode locationMode = Repository.LOCATION_MODE_SETTING.get(metadata.settings());
        if (READONLY_SETTING.exists(metadata.settings())) {
            this.readonly = READONLY_SETTING.get(metadata.settings());
        } else {
            this.readonly = locationMode == LocationMode.SECONDARY_ONLY;
        }
    }

    private static Map<String, String> buildLocation(RepositoryMetadata metadata) {
        return Map.of(
            "base_path",
            Repository.BASE_PATH_SETTING.get(metadata.settings()),
            "container",
            Repository.CONTAINER_SETTING.get(metadata.settings())
        );
    }

    @Override
    protected BlobStore getBlobStore() {
        return super.getBlobStore();
    }

    @Override
    protected AzureBlobStore createBlobStore() {
        final AzureBlobStore blobStore = new AzureBlobStore(metadata, storageService, threadPool);

        logger.debug(
            () -> new ParameterizedMessage(
                "using container [{}], chunk_size [{}], compress [{}], base_path [{}]",
                blobStore,
                chunkSize,
                isCompress(),
                basePath
            )
        );

        return blobStore;
    }

    @Override
    public BlobPath basePath() {
        return basePath;
    }

    @Override
    protected ByteSizeValue chunkSize() {
        return chunkSize;
    }

    @Override
    public boolean isReadOnly() {
        return readonly;
    }

    @Override
    public List<Setting<?>> getRestrictedSystemRepositorySettings() {
        List<Setting<?>> restrictedSettings = new ArrayList<>();
        restrictedSettings.addAll(super.getRestrictedSystemRepositorySettings());
        restrictedSettings.add(Repository.BASE_PATH_SETTING);
        restrictedSettings.add(Repository.LOCATION_MODE_SETTING);
        return restrictedSettings;
    }
}
