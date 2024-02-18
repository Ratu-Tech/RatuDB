/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import org.opensearch.common.Nullable;

/**
 * The holder of the AmazonS3 and AWSCredentialsProvider
 */
final class AmazonAsyncS3WithCredentials {
    private final S3AsyncClient client;
    private final S3AsyncClient priorityClient;
    private final S3AsyncClient urgentClient;
    private final AwsCredentialsProvider credentials;

    private AmazonAsyncS3WithCredentials(
        final S3AsyncClient client,
        final S3AsyncClient priorityClient,
        final S3AsyncClient urgentClient,
        @Nullable final AwsCredentialsProvider credentials
    ) {
        this.client = client;
        this.credentials = credentials;
        this.priorityClient = priorityClient;
        this.urgentClient = urgentClient;
    }

    S3AsyncClient client() {
        return client;
    }

    S3AsyncClient priorityClient() {
        return priorityClient;
    }

    S3AsyncClient urgentClient() {
        return urgentClient;
    }

    AwsCredentialsProvider credentials() {
        return credentials;
    }

    static AmazonAsyncS3WithCredentials create(
        final S3AsyncClient client,
        final S3AsyncClient priorityClient,
        final S3AsyncClient urgentClient,
        @Nullable final AwsCredentialsProvider credentials
    ) {
        return new AmazonAsyncS3WithCredentials(client, priorityClient, urgentClient, credentials);
    }
}
