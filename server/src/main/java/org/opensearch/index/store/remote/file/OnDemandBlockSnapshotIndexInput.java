/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.file;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.opensearch.index.store.remote.utils.BlobFetchRequest;
import org.opensearch.index.store.remote.utils.TransferManager;

import java.io.IOException;

/**
 * This is an implementation of {@link OnDemandBlockIndexInput} where this class provides the main IndexInput using shard snapshot files.
 * <br>
 * This class rely on {@link TransferManager} to really fetch the snapshot files from the remote blob store and maybe cache them
 *
 * @opensearch.internal
 */
public class OnDemandBlockSnapshotIndexInput extends OnDemandBlockIndexInput {
    /**
     * Where this class fetches IndexInput parts from
     */
    final TransferManager transferManager;

    /**
     * FileInfo contains snapshot metadata references for this IndexInput
     */
    protected final FileInfo fileInfo;

    /**
     * Underlying lucene directory to open blocks and for caching
     */
    protected final FSDirectory directory;
    /**
     * File Name
     */
    protected final String fileName;

    /**
     * Maximum size in bytes of snapshot file parts.
     */
    protected final long partSize;

    /**
     * Size of the file, larger than length if it's a slice
     */
    protected final long originalFileSize;

    public OnDemandBlockSnapshotIndexInput(FileInfo fileInfo, FSDirectory directory, TransferManager transferManager) {
        this(
            "BlockedSnapshotIndexInput(path=\""
                + directory.getDirectory().toString()
                + "/"
                + fileInfo.physicalName()
                + "\", "
                + "offset="
                + 0
                + ", length= "
                + fileInfo.length()
                + ")",
            fileInfo,
            0L,
            fileInfo.length(),
            false,
            directory,
            transferManager
        );
    }

    public OnDemandBlockSnapshotIndexInput(
        String resourceDescription,
        FileInfo fileInfo,
        long offset,
        long length,
        boolean isClone,
        FSDirectory directory,
        TransferManager transferManager
    ) {
        this(
            OnDemandBlockIndexInput.builder().resourceDescription(resourceDescription).isClone(isClone).offset(offset).length(length),
            fileInfo,
            directory,
            transferManager
        );
    }

    OnDemandBlockSnapshotIndexInput(
        OnDemandBlockIndexInput.Builder builder,
        FileInfo fileInfo,
        FSDirectory directory,
        TransferManager transferManager
    ) {
        super(builder);
        this.transferManager = transferManager;
        this.fileInfo = fileInfo;
        if (fileInfo.partSize() != null) {
            this.partSize = fileInfo.partSize().getBytes();
        } else {
            // Repository implementations can define a size at which to split files
            // into multiple objects in the repository. If partSize() is null, then
            // no splitting happens, so default to Long.MAX_VALUE here to have the
            // same effect. See {@code BlobStoreRepository#chunkSize()}.
            this.partSize = Long.MAX_VALUE;
        }
        this.fileName = fileInfo.physicalName();
        this.directory = directory;
        this.originalFileSize = fileInfo.length();
    }

    @Override
    protected OnDemandBlockSnapshotIndexInput buildSlice(String sliceDescription, long offset, long length) {
        return new OnDemandBlockSnapshotIndexInput(
            OnDemandBlockIndexInput.builder()
                .blockSizeShift(blockSizeShift)
                .isClone(true)
                .offset(this.offset + offset)
                .length(length)
                .resourceDescription(sliceDescription),
            fileInfo,
            directory,
            transferManager
        );
    }

    @Override
    protected IndexInput fetchBlock(int blockId) throws IOException {
        final String blockFileName = fileName + "." + blockId;

        final long blockStart = getBlockStart(blockId);
        final long blockEnd = blockStart + getActualBlockSize(blockId);

        // If the snapshot file is chunked, we must account for this by
        // choosing the appropriate file part and updating the position
        // accordingly.
        final int part = (int) (blockStart / partSize);
        final long partStart = part * partSize;

        final long position = blockStart - partStart;
        final long length = blockEnd - blockStart;

        BlobFetchRequest blobFetchRequest = BlobFetchRequest.builder()
            .position(position)
            .length(length)
            .blobName(fileInfo.partName(part))
            .directory(directory)
            .fileName(blockFileName)
            .build();
        return transferManager.fetchBlob(blobFetchRequest);
    }

    @Override
    public OnDemandBlockSnapshotIndexInput clone() {
        OnDemandBlockSnapshotIndexInput clone = buildSlice("clone", 0L, this.length);
        // ensures that clones may be positioned at the same point as the blocked file they were cloned from
        clone.cloneBlock(this);
        return clone;
    }

    protected long getActualBlockSize(int blockId) {
        return (blockId != getBlock(originalFileSize - 1)) ? blockSize : getBlockOffset(originalFileSize - 1) + 1;
    }
}
