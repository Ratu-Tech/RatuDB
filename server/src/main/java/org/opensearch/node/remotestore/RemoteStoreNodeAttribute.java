/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node.remotestore;

import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.node.Node;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This is an abstraction for validating and storing information specific to remote backed storage nodes.
 *
 * @opensearch.internal
 */
public class RemoteStoreNodeAttribute {

    public static final String REMOTE_STORE_NODE_ATTRIBUTE_KEY_PREFIX = "remote_store";
    public static final String REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY = "remote_store.segment.repository";
    public static final String REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY = "remote_store.translog.repository";
    public static final String REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY = "remote_store.state.repository";
    public static final String REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT = "remote_store.repository.%s.type";
    public static final String REMOTE_STORE_REPOSITORY_CRYPTO_ATTRIBUTE_KEY_FORMAT = "remote_store.repository.%s."
        + CryptoMetadata.CRYPTO_METADATA_KEY;
    public static final String REMOTE_STORE_REPOSITORY_CRYPTO_SETTINGS_PREFIX = REMOTE_STORE_REPOSITORY_CRYPTO_ATTRIBUTE_KEY_FORMAT
        + "."
        + CryptoMetadata.SETTINGS_KEY;
    public static final String REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX = "remote_store.repository.%s.settings.";
    private final RepositoriesMetadata repositoriesMetadata;

    /**
     * Creates a new {@link RemoteStoreNodeAttribute}
     */
    public RemoteStoreNodeAttribute(DiscoveryNode node) {
        this.repositoriesMetadata = buildRepositoriesMetadata(node);
    }

    private String validateAttributeNonNull(DiscoveryNode node, String attributeKey) {
        String attributeValue = node.getAttributes().get(attributeKey);
        if (attributeValue == null || attributeValue.isEmpty()) {
            throw new IllegalStateException("joining node [" + node + "] doesn't have the node attribute [" + attributeKey + "]");
        }

        return attributeValue;
    }

    private CryptoMetadata buildCryptoMetadata(DiscoveryNode node, String repositoryName) {
        String metadataKey = String.format(Locale.getDefault(), REMOTE_STORE_REPOSITORY_CRYPTO_ATTRIBUTE_KEY_FORMAT, repositoryName);
        boolean isRepoEncrypted = node.getAttributes().keySet().stream().anyMatch(key -> key.startsWith(metadataKey));
        if (isRepoEncrypted == false) {
            return null;
        }

        String keyProviderName = validateAttributeNonNull(node, metadataKey + "." + CryptoMetadata.KEY_PROVIDER_NAME_KEY);
        String keyProviderType = validateAttributeNonNull(node, metadataKey + "." + CryptoMetadata.KEY_PROVIDER_TYPE_KEY);

        String settingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            REMOTE_STORE_REPOSITORY_CRYPTO_SETTINGS_PREFIX,
            repositoryName
        );

        Map<String, String> settingsMap = node.getAttributes()
            .keySet()
            .stream()
            .filter(key -> key.startsWith(settingsAttributeKeyPrefix))
            .collect(Collectors.toMap(key -> key.replace(settingsAttributeKeyPrefix + ".", ""), key -> node.getAttributes().get(key)));

        Settings.Builder settings = Settings.builder();
        settingsMap.forEach(settings::put);

        return new CryptoMetadata(keyProviderName, keyProviderType, settings.build());
    }

    private Map<String, String> validateSettingsAttributesNonNull(DiscoveryNode node, String repositoryName) {
        String settingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            repositoryName
        );
        Map<String, String> settingsMap = node.getAttributes()
            .keySet()
            .stream()
            .filter(key -> key.startsWith(settingsAttributeKeyPrefix))
            .collect(Collectors.toMap(key -> key.replace(settingsAttributeKeyPrefix, ""), key -> validateAttributeNonNull(node, key)));

        if (settingsMap.isEmpty()) {
            throw new IllegalStateException(
                "joining node [" + node + "] doesn't have settings attribute for [" + repositoryName + "] repository"
            );
        }

        return settingsMap;
    }

    private RepositoryMetadata buildRepositoryMetadata(DiscoveryNode node, String name) {
        String type = validateAttributeNonNull(
            node,
            String.format(Locale.getDefault(), REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT, name)
        );
        Map<String, String> settingsMap = validateSettingsAttributesNonNull(node, name);

        Settings.Builder settings = Settings.builder();
        settingsMap.forEach(settings::put);

        CryptoMetadata cryptoMetadata = buildCryptoMetadata(node, name);

        // Repository metadata built here will always be for a system repository.
        settings.put(BlobStoreRepository.SYSTEM_REPOSITORY_SETTING.getKey(), true);

        return new RepositoryMetadata(name, type, settings.build(), cryptoMetadata);
    }

    private RepositoriesMetadata buildRepositoriesMetadata(DiscoveryNode node) {
        List<RepositoryMetadata> repositoryMetadataList = new ArrayList<>();
        Set<String> repositoryNames = new HashSet<>();

        repositoryNames.add(validateAttributeNonNull(node, REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY));
        repositoryNames.add(validateAttributeNonNull(node, REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY));
        repositoryNames.add(validateAttributeNonNull(node, REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY));

        for (String repositoryName : repositoryNames) {
            repositoryMetadataList.add(buildRepositoryMetadata(node, repositoryName));
        }

        return new RepositoriesMetadata(repositoryMetadataList);
    }

    public static boolean isRemoteStoreAttributePresent(Settings settings) {
        return settings.getByPrefix(Node.NODE_ATTRIBUTES.getKey() + REMOTE_STORE_NODE_ATTRIBUTE_KEY_PREFIX).isEmpty() == false;
    }

    public static boolean isRemoteStoreClusterStateEnabled(Settings settings) {
        return RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.get(settings) && isRemoteStoreAttributePresent(settings);
    }

    public RepositoriesMetadata getRepositoriesMetadata() {
        return this.repositoriesMetadata;
    }

    @Override
    public int hashCode() {
        // The hashCode is generated by computing the hash of all the repositoryMetadata present in
        // repositoriesMetadata without generation. Below is the modified list hashCode generation logic.

        int hashCode = 1;
        Iterator iterator = this.repositoriesMetadata.repositories().iterator();
        while (iterator.hasNext()) {
            RepositoryMetadata repositoryMetadata = (RepositoryMetadata) iterator.next();
            hashCode = 31 * hashCode + (repositoryMetadata == null
                ? 0
                : Objects.hash(repositoryMetadata.name(), repositoryMetadata.type(), repositoryMetadata.settings()));
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RemoteStoreNodeAttribute that = (RemoteStoreNodeAttribute) o;

        return this.getRepositoriesMetadata().equalsIgnoreGenerations(that.getRepositoriesMetadata());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('{').append(this.repositoriesMetadata).append('}');
        return super.toString();
    }
}
