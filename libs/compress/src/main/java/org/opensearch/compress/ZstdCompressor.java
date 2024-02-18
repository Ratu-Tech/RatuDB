/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.compress;

import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.ZstdInputStreamNoFinalizer;
import com.github.luben.zstd.ZstdOutputStreamNoFinalizer;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.compress.Compressor;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * {@link Compressor} implementation based on the ZSTD compression algorithm.
 *
 * @opensearch.api - registered name requires BWC support
 * @opensearch.experimental - class methods might change
 */
public class ZstdCompressor implements Compressor {

    /**
     * An arbitrary header that we use to identify compressed streams
     * It needs to be different from other compressors and to not be specific
     * enough so that no stream starting with these bytes could be detected as
     * a XContent
     * */
    private static final byte[] HEADER = new byte[] { 'Z', 'S', 'T', 'D', '\0' };

    /**
     * The name to register the compressor by
     *
     * @opensearch.api - requires BWC support
     */
    @PublicApi(since = "2.10.0")
    public static final String NAME = "ZSTD";

    /**
     * The compression level for {@link ZstdOutputStreamNoFinalizer}
     */
    private static final int LEVEL = 3;

    /** The buffer size for {@link BufferedInputStream} and {@link BufferedOutputStream}
     */
    private static final int BUFFER_SIZE = 4096;

    /**
     * Compares the given bytes with the {@link ZstdCompressor#HEADER} of a compressed stream
     * @param bytes the bytes to compare to ({@link ZstdCompressor#HEADER})
     * @return true if the bytes are the {@link ZstdCompressor#HEADER}, false otherwise
     */
    @Override
    public boolean isCompressed(BytesReference bytes) {
        if (bytes.length() < HEADER.length) {
            return false;
        }
        for (int i = 0; i < HEADER.length; ++i) {
            if (bytes.get(i) != HEADER[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the length of the {@link ZstdCompressor#HEADER}
     * @return the {@link ZstdCompressor#HEADER} length
     */
    @Override
    public int headerLength() {
        return HEADER.length;
    }

    /**
     * Returns a new {@link ZstdInputStreamNoFinalizer} from the given compressed {@link InputStream}
     * @param in the compressed {@link InputStream}
     * @return a new {@link ZstdInputStreamNoFinalizer} from the given compressed {@link InputStream}
     * @throws IOException if an I/O error occurs
     * @throws IllegalArgumentException if the input stream is not compressed with ZSTD
     */
    @Override
    public InputStream threadLocalInputStream(InputStream in) throws IOException {
        final byte[] header = in.readNBytes(HEADER.length);
        if (Arrays.equals(header, HEADER) == false) {
            throw new IllegalArgumentException("Input stream is not compressed with ZSTD!");
        }
        return new ZstdInputStreamNoFinalizer(new BufferedInputStream(in, BUFFER_SIZE), RecyclingBufferPool.INSTANCE);
    }

    /**
     * Returns a new {@link ZstdOutputStreamNoFinalizer} from the given {@link OutputStream}
     * @param out the {@link OutputStream}
     * @return a new {@link ZstdOutputStreamNoFinalizer} from the given {@link OutputStream}
     * @throws IOException if an I/O error occurs
     */
    @Override
    public OutputStream threadLocalOutputStream(OutputStream out) throws IOException {
        out.write(HEADER);
        return new ZstdOutputStreamNoFinalizer(new BufferedOutputStream(out, BUFFER_SIZE), RecyclingBufferPool.INSTANCE, LEVEL);
    }

    /**
     * Always throws an {@link UnsupportedOperationException} as ZSTD compression is supported only for snapshotting
     * @param bytesReference a reference to the bytes to uncompress
     * @return always throws an exception
     * @throws UnsupportedOperationException if the method is called
     * @throws IOException is never thrown
     */
    @Override
    public BytesReference uncompress(BytesReference bytesReference) throws IOException {
        throw new UnsupportedOperationException("ZSTD compression is supported only for snapshotting");
    }

    /**
     * Always throws an {@link UnsupportedOperationException} as ZSTD compression is supported only for snapshotting
     * @param bytesReference a reference to the bytes to compress
     * @return always throws an exception
     * @throws UnsupportedOperationException if the method is called
     */
    @Override
    public BytesReference compress(BytesReference bytesReference) throws IOException {
        throw new UnsupportedOperationException("ZSTD compression is supported only for snapshotting");
    }
}
