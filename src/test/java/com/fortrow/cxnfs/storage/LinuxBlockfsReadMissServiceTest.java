package com.fortrow.cxnfs.storage;

import com.fortrow.cxnfs.hash.Hashing;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class LinuxBlockfsReadMissServiceTest {
    @TempDir
    Path tempDir;

    @Test
    void resolvesKernelMissByFetchingVerifyingWritingAndPublishingExtents() throws Exception {
        byte[] first = bytes("first training chunk");
        byte[] second = bytes("second training chunk");
        QueueRunner runner = new QueueRunner(
                new LinuxBlockfsControlCommand.CommandResult(
                        0,
                        "miss_event event_id=1 reason=read inode=77 offset=8 length=32 "
                                + "dropped_events=0 mount_id=7 metadata_root=/var/lib/cxnfs/vm-smoke\n",
                        ""
                ),
                registeredExtent(77, 0, 1, 0, first.length),
                registeredExtent(77, first.length, 2, 128, second.length),
                new LinuxBlockfsControlCommand.CommandResult(0, "miss_event=none errno=11\n", "")
        );
        Path payloadDevice = tempDir.resolve("payload.img");
        LinuxBlockfsReadMissService service = service(runner, payloadDevice, first, second);

        List<LinuxBlockfsReadMissResolution> results = service.drainAvailable(8, false);

        assertEquals(1, results.size());
        LinuxBlockfsReadMissResolution result = results.getFirst();
        assertTrue(result.fullyPublished());
        assertEquals(2, result.plannedChunks());
        assertEquals(2, result.fetchedChunks());
        assertEquals(2, result.publishedExtents());
        assertEquals(first.length + second.length, result.bytesWritten());
        assertArrayEquals(first, readAt(payloadDevice, 4096, first.length));
        assertArrayEquals(second, readAt(payloadDevice, 8192 + 128, second.length));
        assertEquals("next-miss", runner.commands.get(0).get(3));
        assertEquals("--nonblock", runner.commands.get(0).get(4));
        assertEquals("register-extent", runner.commands.get(1).get(3));
        assertEquals("--metadata-root", runner.commands.get(1).get(4));
        assertEquals("/var/lib/cxnfs/vm-smoke", runner.commands.get(1).get(5));
        assertEquals("0", runner.commands.get(1).get(9));
        assertEquals("register-extent", runner.commands.get(2).get(3));
        assertEquals("--metadata-root", runner.commands.get(2).get(4));
        assertEquals("/var/lib/cxnfs/vm-smoke", runner.commands.get(2).get(5));
        assertEquals(Long.toString(first.length), runner.commands.get(2).get(9));
        assertEquals("next-miss", runner.commands.get(3).get(3));
    }

    @Test
    void fetchesMissChunksAcrossConfiguredCordsBeforePublishingExtents() throws Exception {
        byte[] first = bytes("parallel first training chunk");
        byte[] second = bytes("parallel second training chunk");
        QueueRunner runner = new QueueRunner(
                registeredExtent(77, 0, 1, 0, first.length),
                registeredExtent(77, first.length, 2, 128, second.length)
        );
        LinuxBlockfsManifestIndexSnapshot manifestIndex = manifestIndex(first, second);
        LinuxBlockfsPayloadAllocationSnapshot allocations = new LinuxBlockfsPayloadAllocationSnapshot(
                12,
                List.of(
                        new LinuxBlockfsPayloadAllocation(77, 0, 1, 0, first.length),
                        new LinuxBlockfsPayloadAllocation(77, first.length, 2, 128, second.length)
                )
        );
        CountDownLatch bothFetchesStarted = new CountDownLatch(2);
        LinuxBlockfsChunkFetcher fetcher = (entry, chunk) -> {
            bothFetchesStarted.countDown();
            if (!bothFetchesStarted.await(2, TimeUnit.SECONDS)) {
                throw new IOException("miss chunks were not fetched concurrently");
            }
            if (chunk.fileOffset() == 0) {
                return first;
            }
            if (chunk.fileOffset() == first.length) {
                return second;
            }
            throw new IOException("unexpected chunk request");
        };
        LinuxBlockfsReadMissService service = new LinuxBlockfsReadMissService(
                new LinuxBlockfsControlCommand(Path.of("cxnfs-blockfs-ctl"), Path.of("/dev/test"), runner),
                new LinuxBlockfsReadMissPlanner(manifestIndex, allocations),
                new LinuxBlockfsPayloadCacheWriter(tempDir.resolve("parallel-payload.img")),
                fetcher,
                2
        );

        LinuxBlockfsReadMissResolution result = service.resolve(
                new LinuxBlockfsMissEvent(6, "readahead", 77, 0, first.length + second.length,
                        0, 4, "", 8, 32, 0, true, 1),
                false
        );

        assertEquals(2, service.cordCount());
        assertTrue(result.fullyPublished());
        assertEquals(2, runner.commands.size());
        assertEquals("register-extent", runner.commands.get(0).get(3));
        assertEquals("--mount-id", runner.commands.get(0).get(4));
        assertEquals("4", runner.commands.get(0).get(5));
        assertEquals("register-extent", runner.commands.get(1).get(3));
        assertEquals("--mount-id", runner.commands.get(1).get(4));
        assertEquals("4", runner.commands.get(1).get(5));
    }

    @Test
    void selectorlessMissFallsBackToConfiguredResolverMetadataRoot() throws Exception {
        byte[] first = bytes("configured resolver chunk");
        byte[] second = bytes("unused second chunk");
        QueueRunner runner = new QueueRunner(registeredExtent(77, 0, 1, 0, first.length));
        LinuxBlockfsReadMissService service = service(
                runner,
                tempDir.resolve("payload-configured.img"),
                first,
                second,
                second,
                LinuxBlockfsMountSelector.metadataRoot("/var/lib/cxnfs/disk-a")
        );

        LinuxBlockfsReadMissResolution result = service.resolve(
                new LinuxBlockfsMissEvent(8, "read", 77, 0, first.length, 0),
                false
        );

        assertTrue(result.fullyPublished());
        assertEquals(1, runner.commands.size());
        assertEquals("register-extent", runner.commands.getFirst().get(3));
        assertEquals("--metadata-root", runner.commands.getFirst().get(4));
        assertEquals("/var/lib/cxnfs/disk-a", runner.commands.getFirst().get(5));
    }

    @Test
    void drainsOnlyConfiguredResolverMetadataRoot() throws Exception {
        byte[] first = bytes("configured resolver chunk");
        byte[] second = bytes("unused second chunk");
        QueueRunner runner = new QueueRunner(new LinuxBlockfsControlCommand.CommandResult(
                0,
                "miss_event=none errno=11\n",
                ""
        ));
        LinuxBlockfsReadMissService service = service(
                runner,
                tempDir.resolve("payload-scoped-drain.img"),
                first,
                second,
                second,
                LinuxBlockfsMountSelector.metadataRoot("/var/lib/cxnfs/disk-a")
        );

        List<LinuxBlockfsReadMissResolution> results = service.drainAvailable(8, false);

        assertTrue(results.isEmpty());
        assertEquals("next-miss", runner.commands.getFirst().get(3));
        assertEquals("--metadata-root", runner.commands.getFirst().get(4));
        assertEquals("/var/lib/cxnfs/disk-a", runner.commands.getFirst().get(5));
        assertEquals("--nonblock", runner.commands.getFirst().get(6));
    }

    @Test
    void rejectsMissForDifferentMetadataRootBeforeFetchingOrWritingPayload() {
        byte[] first = bytes("configured resolver chunk");
        byte[] second = bytes("unused second chunk");
        LinuxBlockfsReadMissService service = service(
                command -> {
                    throw new AssertionError("control command should not be called for mismatched scope");
                },
                tempDir.resolve("payload-mismatched.img"),
                first,
                second,
                second,
                LinuxBlockfsMountSelector.metadataRoot("/var/lib/cxnfs/disk-a")
        );
        LinuxBlockfsMissEvent event = new LinuxBlockfsMissEvent(
                9,
                "read",
                77,
                0,
                first.length,
                0,
                0,
                "/var/lib/cxnfs/disk-b",
                16,
                32,
                5000,
                true,
                0
        );

        IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> service.resolve(event, false)
        );

        assertEquals(
                "miss event metadata root /var/lib/cxnfs/disk-b "
                        + "does not match resolver metadata root /var/lib/cxnfs/disk-a",
                error.getMessage()
        );
    }

    @Test
    void rejectsFetchedChunkBeforePayloadCacheWriteWhenHashDoesNotMatch() {
        byte[] first = bytes("first training chunk");
        byte[] second = bytes("second training chunk");
        LinuxBlockfsReadMissService service = service(
                command -> {
                    throw new AssertionError("control command should not be called after failed verification");
                },
                tempDir.resolve("payload.img"),
                first,
                second,
                sameLengthDifferentBytes(second)
        );
        LinuxBlockfsMissEvent event = new LinuxBlockfsMissEvent(4, "read", 77, first.length, second.length, 0);

        IOException error = assertThrows(
                IOException.class,
                () -> service.resolve(event, false)
        );

        assertEquals("fetched chunk hash does not match manifest", error.getMessage());
    }

    @Test
    void returnsEmptyResolutionForStaleMissOutsideManifestRange() throws Exception {
        byte[] first = bytes("first training chunk");
        byte[] second = bytes("second training chunk");
        LinuxBlockfsReadMissService service = service(
                command -> {
                    throw new AssertionError("control command should not be called for stale event");
                },
                tempDir.resolve("payload.img"),
                first,
                second
        );
        LinuxBlockfsMissEvent event = new LinuxBlockfsMissEvent(5, "read", 77, 999_999, 4096, 0);

        LinuxBlockfsReadMissResolution result = service.resolve(event, false);

        assertEquals(0, result.plannedChunks());
        assertEquals(0, result.fetchedChunks());
        assertEquals(0, result.publishedExtents());
        assertEquals(0, result.bytesWritten());
    }

    private static LinuxBlockfsReadMissService service(
            LinuxBlockfsControlCommand.CommandRunner runner,
            Path payloadDevice,
            byte[] first,
            byte[] second
    ) {
        return service(runner, payloadDevice, first, second, second);
    }

    private static LinuxBlockfsReadMissService service(
            LinuxBlockfsControlCommand.CommandRunner runner,
            Path payloadDevice,
            byte[] first,
            byte[] second,
            byte[] fetchedSecond
    ) {
        return service(
                runner,
                payloadDevice,
                first,
                second,
                fetchedSecond,
                LinuxBlockfsMountSelector.unselected()
        );
    }

    private static LinuxBlockfsReadMissService service(
            LinuxBlockfsControlCommand.CommandRunner runner,
            Path payloadDevice,
            byte[] first,
            byte[] second,
            byte[] fetchedSecond,
            LinuxBlockfsMountSelector expectedSelector
    ) {
        LinuxBlockfsManifestIndexSnapshot manifestIndex = manifestIndex(first, second);
        LinuxBlockfsPayloadAllocationSnapshot allocations = new LinuxBlockfsPayloadAllocationSnapshot(
                12,
                List.of(
                        new LinuxBlockfsPayloadAllocation(77, 0, 1, 0, first.length),
                        new LinuxBlockfsPayloadAllocation(77, first.length, 2, 128, second.length)
                )
        );
        LinuxBlockfsReadMissPlanner planner = new LinuxBlockfsReadMissPlanner(manifestIndex, allocations);
        LinuxBlockfsChunkFetcher fetcher = (entry, chunk) -> {
            if (chunk.fileOffset() == 0) {
                return first;
            }
            if (chunk.fileOffset() == first.length) {
                return fetchedSecond;
            }
            throw new IOException("unexpected chunk request");
        };
        return new LinuxBlockfsReadMissService(
                new LinuxBlockfsControlCommand(Path.of("cxnfs-blockfs-ctl"), Path.of("/dev/test"), runner),
                planner,
                new LinuxBlockfsPayloadCacheWriter(payloadDevice),
                fetcher,
                LinuxBlockfsReadMissService.DEFAULT_CORD_COUNT,
                expectedSelector
        );
    }

    private static LinuxBlockfsManifestIndexSnapshot manifestIndex(byte[] first, byte[] second) {
        return new LinuxBlockfsManifestIndexSnapshot(
                12,
                List.of(new LinuxBlockfsManifestEntry(
                        77,
                        "datasets/train/shard-00000.mds",
                        first.length + second.length,
                        Hashing.file(
                                List.of(Hashing.chunk(first), Hashing.chunk(second)),
                                List.of(first.length, second.length)
                        ),
                        List.of(
                                new LinuxBlockfsManifestChunk(0, first.length, Hashing.chunk(first)),
                                new LinuxBlockfsManifestChunk(first.length, second.length, Hashing.chunk(second))
                        )
                ))
        );
    }

    private static LinuxBlockfsControlCommand.CommandResult registeredExtent(
            long inode,
            long fileOffset,
            long payloadBlock,
            int payloadOffset,
            int payloadLength
    ) {
        return new LinuxBlockfsControlCommand.CommandResult(
                0,
                "registered_extent inode="
                        + inode
                        + " file_offset="
                        + fileOffset
                        + " payload_block="
                        + payloadBlock
                        + " payload_offset="
                        + payloadOffset
                        + " payload_length="
                        + payloadLength
                        + "\n",
                ""
        );
    }

    private static byte[] readAt(Path path, long offset, int length) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(length);
        try (SeekableByteChannel channel = Files.newByteChannel(path, StandardOpenOption.READ)) {
            channel.position(offset);
            while (buffer.hasRemaining()) {
                int read = channel.read(buffer);
                if (read < 0) {
                    throw new IOException("payload file ended early");
                }
            }
        }
        return buffer.array();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] sameLengthDifferentBytes(byte[] source) {
        byte[] copy = source.clone();
        copy[0] = (byte) (copy[0] ^ 0x7f);
        return copy;
    }

    private static final class QueueRunner implements LinuxBlockfsControlCommand.CommandRunner {
        private final Queue<LinuxBlockfsControlCommand.CommandResult> results = new ArrayDeque<>();
        private final List<List<String>> commands = new ArrayList<>();

        QueueRunner(LinuxBlockfsControlCommand.CommandResult... results) {
            this.results.addAll(List.of(results));
        }

        @Override
        public LinuxBlockfsControlCommand.CommandResult run(List<String> command) {
            commands.add(command);
            if (results.isEmpty()) {
                throw new AssertionError("unexpected command: " + command);
            }
            return results.remove();
        }
    }
}
