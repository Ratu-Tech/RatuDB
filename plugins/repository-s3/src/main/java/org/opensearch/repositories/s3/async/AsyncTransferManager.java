/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.async;

import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.utils.CompletableFutureUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.StreamContext;
import org.opensearch.common.blobstore.exception.CorruptFileException;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.common.util.ByteUtils;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.repositories.s3.SocketAccess;
import org.opensearch.repositories.s3.StatsMetricPublisher;
import org.opensearch.repositories.s3.io.CheckedContainer;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import com.jcraft.jzlib.JZlib;

/**
 * A helper class that automatically uses multipart upload based on the size of the source object
 */
public final class AsyncTransferManager {
    private static final Logger log = LogManager.getLogger(AsyncTransferManager.class);
    private final ExecutorService executorService;
    private final ExecutorService priorityExecutorService;
    private final ExecutorService urgentExecutorService;
    private final long minimumPartSize;

    /**
     * The max number of parts on S3 side is 10,000
     */
    private static final long MAX_UPLOAD_PARTS = 10_000;

    /**
     * Construct a new object of AsyncTransferManager
     *
     * @param minimumPartSize         The minimum part size for parallel multipart uploads
     * @param executorService         The stream reader {@link ExecutorService} for normal priority uploads
     * @param priorityExecutorService The stream read {@link ExecutorService} for high priority uploads
     */
    public AsyncTransferManager(
        long minimumPartSize,
        ExecutorService executorService,
        ExecutorService priorityExecutorService,
        ExecutorService urgentExecutorService
    ) {
        this.executorService = executorService;
        this.priorityExecutorService = priorityExecutorService;
        this.minimumPartSize = minimumPartSize;
        this.urgentExecutorService = urgentExecutorService;
    }

    /**
     * Upload an object to S3 using the async client
     *
     * @param s3AsyncClient S3 client to use for upload
     * @param uploadRequest The {@link UploadRequest} object encapsulating all relevant details for upload
     * @param streamContext The {@link StreamContext} to supply streams during upload
     * @return A {@link CompletableFuture} to listen for upload completion
     */
    public CompletableFuture<Void> uploadObject(
        S3AsyncClient s3AsyncClient,
        UploadRequest uploadRequest,
        StreamContext streamContext,
        StatsMetricPublisher statsMetricPublisher
    ) {

        CompletableFuture<Void> returnFuture = new CompletableFuture<>();
        try {
            if (streamContext.getNumberOfParts() == 1) {
                log.debug(() -> "Starting the upload as a single upload part request");
                uploadInOneChunk(s3AsyncClient, uploadRequest, streamContext.provideStream(0), returnFuture, statsMetricPublisher);
            } else {
                log.debug(() -> "Starting the upload as multipart upload request");
                uploadInParts(s3AsyncClient, uploadRequest, streamContext, returnFuture, statsMetricPublisher);
            }
        } catch (Throwable throwable) {
            returnFuture.completeExceptionally(throwable);
        }

        return returnFuture;
    }

    private void uploadInParts(
        S3AsyncClient s3AsyncClient,
        UploadRequest uploadRequest,
        StreamContext streamContext,
        CompletableFuture<Void> returnFuture,
        StatsMetricPublisher statsMetricPublisher
    ) {

        CreateMultipartUploadRequest.Builder createMultipartUploadRequestBuilder = CreateMultipartUploadRequest.builder()
            .bucket(uploadRequest.getBucket())
            .key(uploadRequest.getKey())
            .overrideConfiguration(o -> o.addMetricPublisher(statsMetricPublisher.multipartUploadMetricCollector));
        if (uploadRequest.doRemoteDataIntegrityCheck()) {
            createMultipartUploadRequestBuilder.checksumAlgorithm(ChecksumAlgorithm.CRC32);
        }
        CompletableFuture<CreateMultipartUploadResponse> createMultipartUploadFuture = SocketAccess.doPrivileged(
            () -> s3AsyncClient.createMultipartUpload(createMultipartUploadRequestBuilder.build())
        );

        // Ensure cancellations are forwarded to the createMultipartUploadFuture future
        CompletableFutureUtils.forwardExceptionTo(returnFuture, createMultipartUploadFuture);

        createMultipartUploadFuture.whenComplete((createMultipartUploadResponse, throwable) -> {
            if (throwable != null) {
                handleException(returnFuture, () -> "Failed to initiate multipart upload", throwable);
            } else {
                log.debug(() -> "Initiated new multipart upload, uploadId: " + createMultipartUploadResponse.uploadId());
                doUploadInParts(s3AsyncClient, uploadRequest, streamContext, returnFuture, createMultipartUploadResponse.uploadId());
            }
        });
    }

    private void doUploadInParts(
        S3AsyncClient s3AsyncClient,
        UploadRequest uploadRequest,
        StreamContext streamContext,
        CompletableFuture<Void> returnFuture,
        String uploadId
    ) {

        // The list of completed parts must be sorted
        AtomicReferenceArray<CompletedPart> completedParts = new AtomicReferenceArray<>(streamContext.getNumberOfParts());
        AtomicReferenceArray<CheckedContainer> inputStreamContainers = new AtomicReferenceArray<>(streamContext.getNumberOfParts());

        List<CompletableFuture<CompletedPart>> futures;
        try {
            futures = AsyncPartsHandler.uploadParts(
                s3AsyncClient,
                executorService,
                priorityExecutorService,
                urgentExecutorService,
                uploadRequest,
                streamContext,
                uploadId,
                completedParts,
                inputStreamContainers
            );
        } catch (Exception ex) {
            try {
                AsyncPartsHandler.cleanUpParts(s3AsyncClient, uploadRequest, uploadId);
            } finally {
                returnFuture.completeExceptionally(ex);
            }
            return;
        }

        CompletableFutureUtils.allOfExceptionForwarded(futures.toArray(CompletableFuture[]::new)).thenApply(resp -> {
            try {
                uploadRequest.getUploadFinalizer().accept(true);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return resp;
        }).thenApply(ignore -> {
            if (uploadRequest.doRemoteDataIntegrityCheck()) {
                mergeAndVerifyChecksum(inputStreamContainers, uploadRequest.getKey(), uploadRequest.getExpectedChecksum());
            }
            return null;
        })
            .thenCompose(ignore -> completeMultipartUpload(s3AsyncClient, uploadRequest, uploadId, completedParts))
            .handle(handleExceptionOrResponse(s3AsyncClient, uploadRequest, returnFuture, uploadId))
            .exceptionally(throwable -> {
                handleException(returnFuture, () -> "Unexpected exception occurred", throwable);
                return null;
            });
    }

    private void mergeAndVerifyChecksum(
        AtomicReferenceArray<CheckedContainer> inputStreamContainers,
        String fileName,
        long expectedChecksum
    ) {
        long resultantChecksum = fromBase64String(inputStreamContainers.get(0).getChecksum());
        for (int index = 1; index < inputStreamContainers.length(); index++) {
            long curChecksum = fromBase64String(inputStreamContainers.get(index).getChecksum());
            resultantChecksum = JZlib.crc32_combine(resultantChecksum, curChecksum, inputStreamContainers.get(index).getContentLength());
        }

        if (resultantChecksum != expectedChecksum) {
            throw new RuntimeException(new CorruptFileException("File level checksums didn't match combined part checksums", fileName));
        }
    }

    private BiFunction<CompleteMultipartUploadResponse, Throwable, Void> handleExceptionOrResponse(
        S3AsyncClient s3AsyncClient,
        UploadRequest uploadRequest,
        CompletableFuture<Void> returnFuture,
        String uploadId
    ) {

        return (response, throwable) -> {
            if (throwable != null) {
                AsyncPartsHandler.cleanUpParts(s3AsyncClient, uploadRequest, uploadId);
                handleException(returnFuture, () -> "Failed to send multipart upload requests.", throwable);
            } else {
                returnFuture.complete(null);
            }

            return null;
        };
    }

    private CompletableFuture<CompleteMultipartUploadResponse> completeMultipartUpload(
        S3AsyncClient s3AsyncClient,
        UploadRequest uploadRequest,
        String uploadId,
        AtomicReferenceArray<CompletedPart> completedParts
    ) {

        log.debug(() -> new ParameterizedMessage("Sending completeMultipartUploadRequest, uploadId: {}", uploadId));
        CompletedPart[] parts = IntStream.range(0, completedParts.length()).mapToObj(completedParts::get).toArray(CompletedPart[]::new);
        CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
            .bucket(uploadRequest.getBucket())
            .key(uploadRequest.getKey())
            .uploadId(uploadId)
            .multipartUpload(CompletedMultipartUpload.builder().parts(parts).build())
            .build();

        return SocketAccess.doPrivileged(() -> s3AsyncClient.completeMultipartUpload(completeMultipartUploadRequest));
    }

    private static String base64StringFromLong(Long val) {
        return Base64.getEncoder().encodeToString(Arrays.copyOfRange(ByteUtils.toByteArrayBE(val), 4, 8));
    }

    private static long fromBase64String(String base64String) {
        byte[] decodedBytes = Base64.getDecoder().decode(base64String);
        if (decodedBytes.length != 4) {
            throw new IllegalArgumentException("Invalid Base64 encoded CRC32 checksum");
        }
        long result = 0;
        for (int i = 0; i < 4; i++) {
            result <<= 8;
            result |= (decodedBytes[i] & 0xFF);
        }
        return result;
    }

    private static void handleException(CompletableFuture<Void> returnFuture, Supplier<String> message, Throwable throwable) {
        Throwable cause = throwable instanceof CompletionException ? throwable.getCause() : throwable;

        if (cause instanceof Error) {
            returnFuture.completeExceptionally(cause);
        } else {
            SdkClientException exception = SdkClientException.create(message.get(), cause);
            returnFuture.completeExceptionally(exception);
        }
    }

    /**
     * Calculates the optimal part size of each part request if the upload operation is carried out as multipart upload.
     */
    public long calculateOptimalPartSize(long contentLengthOfSource) {
        if (contentLengthOfSource < ByteSizeUnit.MB.toBytes(5)) {
            return contentLengthOfSource;
        }
        double optimalPartSize = contentLengthOfSource / (double) MAX_UPLOAD_PARTS;
        optimalPartSize = Math.ceil(optimalPartSize);
        return (long) Math.max(optimalPartSize, minimumPartSize);
    }

    private void uploadInOneChunk(
        S3AsyncClient s3AsyncClient,
        UploadRequest uploadRequest,
        InputStreamContainer inputStreamContainer,
        CompletableFuture<Void> returnFuture,
        StatsMetricPublisher statsMetricPublisher
    ) {
        PutObjectRequest.Builder putObjectRequestBuilder = PutObjectRequest.builder()
            .bucket(uploadRequest.getBucket())
            .key(uploadRequest.getKey())
            .contentLength(uploadRequest.getContentLength())
            .overrideConfiguration(o -> o.addMetricPublisher(statsMetricPublisher.putObjectMetricPublisher));
        if (uploadRequest.doRemoteDataIntegrityCheck()) {
            putObjectRequestBuilder.checksumAlgorithm(ChecksumAlgorithm.CRC32);
            putObjectRequestBuilder.checksumCRC32(base64StringFromLong(uploadRequest.getExpectedChecksum()));
        }
        ExecutorService streamReadExecutor;
        if (uploadRequest.getWritePriority() == WritePriority.URGENT) {
            streamReadExecutor = urgentExecutorService;
        } else if (uploadRequest.getWritePriority() == WritePriority.HIGH) {
            streamReadExecutor = priorityExecutorService;
        } else {
            streamReadExecutor = executorService;
        }
        // Buffered stream is needed to allow mark and reset ops during IO errors so that only buffered
        // data can be retried instead of retrying whole file by the application.
        InputStream inputStream = new BufferedInputStream(inputStreamContainer.getInputStream(), (int) (ByteSizeUnit.MB.toBytes(1) + 1));
        CompletableFuture<Void> putObjectFuture = SocketAccess.doPrivileged(
            () -> s3AsyncClient.putObject(
                putObjectRequestBuilder.build(),
                AsyncRequestBody.fromInputStream(inputStream, inputStreamContainer.getContentLength(), streamReadExecutor)
            ).handle((resp, throwable) -> {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    log.error(
                        () -> new ParameterizedMessage("Failed to close stream while uploading single file {}.", uploadRequest.getKey()),
                        e
                    );
                }
                if (throwable != null) {
                    Throwable unwrappedThrowable = ExceptionsHelper.unwrap(throwable, S3Exception.class);
                    if (unwrappedThrowable != null) {
                        S3Exception s3Exception = (S3Exception) unwrappedThrowable;
                        if (s3Exception.statusCode() == HttpStatusCode.BAD_REQUEST
                            && "BadDigest".equals(s3Exception.awsErrorDetails().errorCode())) {
                            throw new RuntimeException(new CorruptFileException(s3Exception, uploadRequest.getKey()));
                        }
                    }
                    returnFuture.completeExceptionally(throwable);
                } else {
                    try {
                        uploadRequest.getUploadFinalizer().accept(true);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    returnFuture.complete(null);
                }

                return null;
            }).handle((resp, throwable) -> {
                if (throwable != null) {
                    deleteUploadedObject(s3AsyncClient, uploadRequest);
                    returnFuture.completeExceptionally(throwable);
                }

                return null;
            })
        );

        CompletableFutureUtils.forwardExceptionTo(returnFuture, putObjectFuture);
        CompletableFutureUtils.forwardResultTo(putObjectFuture, returnFuture);
    }

    private void deleteUploadedObject(S3AsyncClient s3AsyncClient, UploadRequest uploadRequest) {
        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
            .bucket(uploadRequest.getBucket())
            .key(uploadRequest.getKey())
            .build();

        SocketAccess.doPrivileged(() -> s3AsyncClient.deleteObject(deleteObjectRequest)).exceptionally(throwable -> {
            log.error(() -> new ParameterizedMessage("Failed to delete uploaded object of key {}", uploadRequest.getKey()), throwable);
            return null;
        });
    }
}
