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

package org.opensearch.index.translog;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.UUIDs;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

public class TranslogHeaderTests extends OpenSearchTestCase {

    public void testCurrentHeaderVersion() throws Exception {
        final String translogUUID = UUIDs.randomBase64UUID();
        final TranslogHeader outHeader = new TranslogHeader(translogUUID, randomNonNegativeLong());
        final long generation = randomNonNegativeLong();
        final Path translogFile = createTempDir().resolve(Translog.getFilename(generation));
        try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            outHeader.write(channel, true);
            assertThat(outHeader.sizeInBytes(), equalTo((int) channel.position()));
        }
        try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.READ)) {
            final TranslogHeader inHeader = TranslogHeader.read(translogUUID, translogFile, channel);
            assertThat(inHeader.getTranslogUUID(), equalTo(translogUUID));
            assertThat(inHeader.getPrimaryTerm(), equalTo(outHeader.getPrimaryTerm()));
            assertThat(inHeader.sizeInBytes(), equalTo((int) channel.position()));
        }
        final TranslogCorruptedException mismatchUUID = expectThrows(TranslogCorruptedException.class, () -> {
            try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.READ)) {
                TranslogHeader.read(randomValueOtherThan(translogUUID, UUIDs::randomBase64UUID), translogFile, channel);
            }
        });
        assertThat(mismatchUUID.getMessage(), containsString("this translog file belongs to a different translog"));
        TestTranslog.corruptFile(logger, random(), translogFile, false);
        final TranslogCorruptedException corruption = expectThrows(TranslogCorruptedException.class, () -> {
            try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.READ)) {
                final TranslogHeader translogHeader = TranslogHeader.read(
                    randomBoolean() ? outHeader.getTranslogUUID() : UUIDs.randomBase64UUID(),
                    translogFile,
                    channel
                );
                // succeeds if the corruption corrupted the version byte making this look like a v2 translog, because we don't check the
                // checksum on this version
                assertThat(
                    "version " + TranslogHeader.VERSION_CHECKPOINTS + " translog",
                    translogHeader.getPrimaryTerm(),
                    equalTo(SequenceNumbers.UNASSIGNED_PRIMARY_TERM)
                );
                throw new TranslogCorruptedException(translogFile.toString(), "adjusted translog version");
            } catch (IllegalStateException e) {
                // corruption corrupted the version byte making this look like a v2, v1 or v0 translog
                assertThat(
                    "version " + TranslogHeader.VERSION_CHECKPOINTS + "-or-earlier translog",
                    e.getMessage(),
                    anyOf(
                        containsString("pre-2.0 translog found"),
                        containsString("pre-1.4 translog found"),
                        containsString("pre-6.3 translog found")
                    )
                );
                throw new TranslogCorruptedException(translogFile.toString(), "adjusted translog version", e);
            }
        });
        assertThat(corruption.getMessage(), not(containsString("this translog file belongs to a different translog")));
    }

    public void testHeaderWithoutPrimaryTerm() throws Exception {
        final String translogUUID = UUIDs.randomBase64UUID();
        final long generation = randomNonNegativeLong();
        final Path translogFile = createTempDir().resolve(Translog.getFilename(generation));
        try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            writeHeaderWithoutTerm(channel, translogUUID);
            assertThat((int) channel.position(), lessThan(TranslogHeader.headerSizeInBytes(translogUUID)));
        }
        try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.READ)) {
            final TranslogHeader inHeader = TranslogHeader.read(translogUUID, translogFile, channel);
            assertThat(inHeader.getTranslogUUID(), equalTo(translogUUID));
            assertThat(inHeader.getPrimaryTerm(), equalTo(SequenceNumbers.UNASSIGNED_PRIMARY_TERM));
            assertThat(inHeader.sizeInBytes(), equalTo((int) channel.position()));
        }
        expectThrows(TranslogCorruptedException.class, () -> {
            try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.READ)) {
                TranslogHeader.read(UUIDs.randomBase64UUID(), translogFile, channel);
            }
        });
    }

    public void testCurrentHeaderVersionWithoutUUIDComparison() throws Exception {
        final String translogUUID = UUIDs.randomBase64UUID();
        final TranslogHeader outHeader = new TranslogHeader(translogUUID, randomNonNegativeLong());
        final long generation = randomNonNegativeLong();
        final Path translogFile = createTempDir().resolve(Translog.getFilename(generation));
        try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            outHeader.write(channel, true);
            assertThat(outHeader.sizeInBytes(), equalTo((int) channel.position()));
        }
        try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.READ)) {
            final TranslogHeader inHeader = TranslogHeader.read(translogFile, channel);
            assertThat(inHeader.getTranslogUUID(), equalTo(translogUUID));
            assertThat(inHeader.getPrimaryTerm(), equalTo(outHeader.getPrimaryTerm()));
            assertThat(inHeader.sizeInBytes(), equalTo((int) channel.position()));
        }

        TestTranslog.corruptFile(logger, random(), translogFile, false);
        final TranslogCorruptedException corruption = expectThrows(TranslogCorruptedException.class, () -> {
            try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.READ)) {
                final TranslogHeader translogHeader = TranslogHeader.read(translogFile, channel);
                assertThat(
                    "version " + TranslogHeader.VERSION_CHECKPOINTS + " translog",
                    translogHeader.getPrimaryTerm(),
                    equalTo(SequenceNumbers.UNASSIGNED_PRIMARY_TERM)
                );
                throw new TranslogCorruptedException(translogFile.toString(), "adjusted translog version");
            } catch (IllegalStateException e) {
                // corruption corrupted the version byte making this look like a v2, v1 or v0 translog
                assertThat(
                    "version " + TranslogHeader.VERSION_CHECKPOINTS + "-or-earlier translog",
                    e.getMessage(),
                    anyOf(
                        containsString("pre-2.0 translog found"),
                        containsString("pre-1.4 translog found"),
                        containsString("pre-6.3 translog found")
                    )
                );
                throw new TranslogCorruptedException(translogFile.toString(), "adjusted translog version", e);
            }
        });
        assertThat(corruption.getMessage(), not(containsString("this translog file belongs to a different translog")));
    }

    static void writeHeaderWithoutTerm(FileChannel channel, String translogUUID) throws IOException {
        final OutputStreamStreamOutput out = new OutputStreamStreamOutput(Channels.newOutputStream(channel));
        CodecUtil.writeHeader(new OutputStreamDataOutput(out), TranslogHeader.TRANSLOG_CODEC, TranslogHeader.VERSION_CHECKPOINTS);
        final BytesRef uuid = new BytesRef(translogUUID);
        out.writeInt(uuid.length);
        out.writeBytes(uuid.bytes, uuid.offset, uuid.length);
        channel.force(true);
        assertThat(channel.position(), equalTo(43L));
    }

    public void testLegacyTranslogVersions() throws Exception {
        checkFailsToOpen("/org/opensearch/index/translog/translog-v0.binary", IllegalStateException.class, "pre-1.4 translog");
        checkFailsToOpen("/org/opensearch/index/translog/translog-v1.binary", IllegalStateException.class, "pre-2.0 translog");
        checkFailsToOpen("/org/opensearch/index/translog/translog-v1-truncated.binary", IllegalStateException.class, "pre-2.0 translog");
        checkFailsToOpen(
            "/org/opensearch/index/translog/translog-v1-corrupted-magic.binary",
            TranslogCorruptedException.class,
            "translog looks like version 1 or later, but has corrupted header"
        );
        checkFailsToOpen(
            "/org/opensearch/index/translog/translog-v1-corrupted-body.binary",
            IllegalStateException.class,
            "pre-2.0 translog"
        );
    }

    public void testCorruptTranslogHeader() throws Exception {
        final String translogUUID = UUIDs.randomBase64UUID();
        final TranslogHeader outHeader = new TranslogHeader(translogUUID, randomNonNegativeLong());
        final long generation = randomNonNegativeLong();
        final Path translogLocation = createTempDir();
        final Path translogFile = translogLocation.resolve(Translog.getFilename(generation));
        try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            outHeader.write(channel, true);
            assertThat(outHeader.sizeInBytes(), equalTo((int) channel.position()));
        }
        TestTranslog.corruptFile(logger, random(), translogFile, false);
        final Exception error = expectThrows(Exception.class, () -> {
            try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.READ)) {
                TranslogHeader.read(randomValueOtherThan(translogUUID, UUIDs::randomBase64UUID), translogFile, channel);
            }
        });
        assertThat(error, either(instanceOf(IllegalStateException.class)).or(instanceOf(TranslogCorruptedException.class)));
    }

    private <E extends Exception> void checkFailsToOpen(String file, Class<E> expectedErrorType, String expectedMessage) {
        final Path translogFile = getDataPath(file);
        assertThat("test file [" + translogFile + "] should exist", Files.exists(translogFile), equalTo(true));
        final E error = expectThrows(expectedErrorType, () -> {
            final Checkpoint checkpoint = new Checkpoint(
                Files.size(translogFile),
                1,
                1,
                SequenceNumbers.NO_OPS_PERFORMED,
                SequenceNumbers.NO_OPS_PERFORMED,
                SequenceNumbers.NO_OPS_PERFORMED,
                1,
                SequenceNumbers.NO_OPS_PERFORMED
            );
            try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.READ)) {
                TranslogReader.open(channel, translogFile, checkpoint, null);
            }
        });
        assertThat(error.getMessage(), containsString(expectedMessage));
    }
}
