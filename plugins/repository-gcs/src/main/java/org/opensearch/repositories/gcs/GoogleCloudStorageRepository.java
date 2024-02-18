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

package org.opensearch.repositories.gcs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.repositories.RepositoryException;
import org.opensearch.repositories.blobstore.MeteredBlobStoreRepository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.opensearch.common.settings.Setting.Property;
import static org.opensearch.common.settings.Setting.byteSizeSetting;
import static org.opensearch.common.settings.Setting.simpleString;

class GoogleCloudStorageRepository extends MeteredBlobStoreRepository {
    private static final Logger logger = LogManager.getLogger(GoogleCloudStorageRepository.class);

    // package private for testing
    static final ByteSizeValue MIN_CHUNK_SIZE = new ByteSizeValue(1, ByteSizeUnit.BYTES);

    /**
     * Maximum allowed object size in GCS.
     * @see <a href="https://cloud.google.com/storage/quotas#objects">GCS documentation</a> for details.
     */
    static final ByteSizeValue MAX_CHUNK_SIZE = new ByteSizeValue(5, ByteSizeUnit.TB);

    static final String TYPE = "gcs";

    static final Setting<String> BUCKET = simpleString("bucket", Property.NodeScope, Property.Dynamic);
    static final Setting<String> BASE_PATH = simpleString("base_path", Property.NodeScope, Property.Dynamic);
    static final Setting<ByteSizeValue> CHUNK_SIZE = byteSizeSetting(
        "chunk_size",
        MAX_CHUNK_SIZE,
        MIN_CHUNK_SIZE,
        MAX_CHUNK_SIZE,
        Property.NodeScope,
        Property.Dynamic
    );
    static final Setting<String> CLIENT_NAME = new Setting<>("client", "default", Function.identity());

    private final GoogleCloudStorageService storageService;
    private final BlobPath basePath;
    private final ByteSizeValue chunkSize;
    private final String bucket;
    private final String clientName;

    GoogleCloudStorageRepository(
        final RepositoryMetadata metadata,
        final NamedXContentRegistry namedXContentRegistry,
        final GoogleCloudStorageService storageService,
        final ClusterService clusterService,
        final RecoverySettings recoverySettings
    ) {
        super(metadata, namedXContentRegistry, clusterService, recoverySettings, buildLocation(metadata));
        this.storageService = storageService;

        String basePath = BASE_PATH.get(metadata.settings());
        if (Strings.hasLength(basePath)) {
            BlobPath path = new BlobPath();
            for (String elem : basePath.split("/")) {
                path = path.add(elem);
            }
            this.basePath = path;
        } else {
            this.basePath = BlobPath.cleanPath();
        }

        this.chunkSize = getSetting(CHUNK_SIZE, metadata);
        this.bucket = getSetting(BUCKET, metadata);
        this.clientName = CLIENT_NAME.get(metadata.settings());
        logger.debug("using bucket [{}], base_path [{}], chunk_size [{}], compress [{}]", bucket, basePath, chunkSize, isCompress());
    }

    private static Map<String, String> buildLocation(RepositoryMetadata metadata) {
        return Map.of("base_path", BASE_PATH.get(metadata.settings()), "bucket", getSetting(BUCKET, metadata));
    }

    @Override
    protected GoogleCloudStorageBlobStore createBlobStore() {
        return new GoogleCloudStorageBlobStore(bucket, clientName, metadata.name(), storageService, bufferSize);
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
    public List<Setting<?>> getRestrictedSystemRepositorySettings() {
        List<Setting<?>> restrictedSettings = new ArrayList<>();
        restrictedSettings.addAll(super.getRestrictedSystemRepositorySettings());
        restrictedSettings.add(BUCKET);
        restrictedSettings.add(BASE_PATH);
        return restrictedSettings;
    }

    /**
     * Get a given setting from the repository settings, throwing a {@link RepositoryException} if the setting does not exist or is empty.
     */
    static <T> T getSetting(Setting<T> setting, RepositoryMetadata metadata) {
        T value = setting.get(metadata.settings());
        if (value == null) {
            throw new RepositoryException(metadata.name(), "Setting [" + setting.getKey() + "] is not defined for repository");
        }
        if ((value instanceof String) && (Strings.hasText((String) value)) == false) {
            throw new RepositoryException(metadata.name(), "Setting [" + setting.getKey() + "] is empty for repository");
        }
        return value;
    }
}
