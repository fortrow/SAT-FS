package com.fortrow.cxnfs.storage;

import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.manifest.ChunkRef;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.performance.ReadAheadPolicy;
import com.fortrow.cxnfs.reconstruction.ReconstructionPlan;
import com.fortrow.cxnfs.reconstruction.ReconstructionTerm;
import com.fortrow.cxnfs.transport.CordPoolConfig;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class LinuxBlockfsExtentPublisherTest {
    @Test
    void publishesResolvedVerifiedExtentsThroughControlCommand() throws Exception {
        QueueRunner runner = new QueueRunner(
                acceptedRegister(),
                acceptedRegister()
        );
        LinuxBlockfsExtentPublisher publisher = publisher(runner, this::resolveExtent);
        EarlyReadStoragePlan plan = plan(true);

        LinuxBlockfsPublishResult result = publisher.publishVerifiedExtents(plan, false);

        assertTrue(result.fullyAccepted());
        assertEquals(2, result.attemptedExtents());
        assertEquals(2, result.acceptedExtents());
        assertEquals(0, result.unsupportedExtents());
        assertEquals(128L * 1024, result.acceptedBytes());
        assertEquals(2, runner.commands.size());
        assertEquals("register-extent", runner.commands.getFirst().get(3));
        assertEquals("77", runner.commands.getFirst().get(5));
        assertEquals("65536", runner.commands.get(1).get(7));
    }

    @Test
    void countsUnsupportedOlderKernelWhenAllowed() throws Exception {
        QueueRunner runner = new QueueRunner(
                unsupportedRegister(),
                unsupportedRegister()
        );
        LinuxBlockfsExtentPublisher publisher = publisher(runner, this::resolveExtent);

        LinuxBlockfsPublishResult result = publisher.publishVerifiedExtents(plan(true), true);

        assertEquals(2, result.attemptedExtents());
        assertEquals(0, result.acceptedExtents());
        assertEquals(2, result.unsupportedExtents());
        assertEquals(0, result.acceptedBytes());
    }

    @Test
    void rejectsPlansThatWouldPublishWithoutTrailingVerification() {
        QueueRunner runner = new QueueRunner();
        LinuxBlockfsExtentPublisher publisher = publisher(runner, this::resolveExtent);

        assertThrows(
                IllegalArgumentException.class,
                () -> publisher.publishVerifiedExtents(plan(false), false)
        );
    }

    @Test
    void invalidatesFileRangeThroughControlCommand() throws Exception {
        QueueRunner runner = new QueueRunner(new LinuxBlockfsControlCommand.CommandResult(
                0,
                "invalidated inode=77 offset=0 length=0\n",
                ""
        ));
        LinuxBlockfsExtentPublisher publisher = publisher(runner, this::resolveExtent);

        LinuxBlockfsControlResult result = publisher.invalidate(77, 0, 0);

        assertTrue(result.accepted());
        assertEquals(List.of(
                "cxnfs-blockfs-ctl",
                "--device",
                "/dev/test",
                "invalidate",
                "--inode",
                "77",
                "--offset",
                "0",
                "--length",
                "0"
        ), runner.commands.getFirst());
    }

    @Test
    void publishesFileNamespaceRecordThroughControlCommand() throws Exception {
        QueueRunner runner = new QueueRunner(new LinuxBlockfsControlCommand.CommandResult(
                0,
                "registered_file inode=77 path=sample.bin size=4096 mode=444\n",
                ""
        ));
        LinuxBlockfsExtentPublisher publisher = publisher(runner, this::resolveExtent);

        LinuxBlockfsControlResult result = publisher.publishFile(
                LinuxBlockfsFile.readOnly(77, "datasets/train/sample.bin", 4096)
        );

        assertTrue(result.accepted());
        assertEquals(List.of(
                "cxnfs-blockfs-ctl",
                "--device",
                "/dev/test",
                "register-file",
                "--inode",
                "77",
                "--path",
                "datasets/train/sample.bin",
                "--size",
                "4096",
                "--mode",
                "0444"
        ), runner.commands.getFirst());
    }

    @Test
    void publishesNamespaceSnapshotThroughControlCommand() throws Exception {
        QueueRunner runner = new QueueRunner(
                registeredFile(77, "datasets/train/sample.bin"),
                registeredFile(78, "datasets/train/labels.mds")
        );
        LinuxBlockfsExtentPublisher publisher = publisher(runner, this::resolveExtent);

        int published = publisher.publishNamespace(new LinuxBlockfsNamespaceSnapshot(
                9,
                List.of(
                        LinuxBlockfsFile.readOnly(77, "datasets/train/sample.bin", 4096),
                        LinuxBlockfsFile.readOnly(78, "datasets/train/labels.mds", 8192)
                )
        ));

        assertEquals(2, published);
        assertEquals("datasets/train/sample.bin", runner.commands.get(0).get(7));
        assertEquals("datasets/train/labels.mds", runner.commands.get(1).get(7));
    }

    @Test
    void canClearNamespaceBeforePublishingReplacementSnapshot() throws Exception {
        QueueRunner runner = new QueueRunner(
                new LinuxBlockfsControlCommand.CommandResult(0, "cleared_namespace=true\n", ""),
                registeredFile(77, "datasets/train/sample.bin")
        );
        LinuxBlockfsExtentPublisher publisher = publisher(runner, this::resolveExtent);

        int published = publisher.publishNamespace(new LinuxBlockfsNamespaceSnapshot(
                9,
                List.of(LinuxBlockfsFile.readOnly(77, "datasets/train/sample.bin", 4096))
        ), true);

        assertEquals(1, published);
        assertEquals("clear-namespace", runner.commands.get(0).get(3));
        assertEquals("register-file", runner.commands.get(1).get(3));
    }

    @Test
    void publishesExtentHintSnapshotThroughControlCommand() throws Exception {
        QueueRunner runner = new QueueRunner(
                acceptedRegister(),
                acceptedRegister()
        );
        LinuxBlockfsExtentPublisher publisher = publisher(runner, this::resolveExtent);

        LinuxBlockfsPublishResult result = publisher.publishExtentHints(
                new LinuxBlockfsExtentHintsSnapshot(
                        11,
                        List.of(
                                new LinuxBlockfsExtent(77, 0, 100, 0, 65536, hash(10).bytes()),
                                new LinuxBlockfsExtent(77, 65536, 116, 0, 65536, hash(11).bytes())
                        )
                ),
                false
        );

        assertTrue(result.fullyAccepted());
        assertEquals(2, runner.commands.size());
        assertEquals("register-extent", runner.commands.get(0).get(3));
        assertEquals("0", runner.commands.get(0).get(7));
        assertEquals("65536", runner.commands.get(1).get(7));
    }

    private LinuxBlockfsExtentPublisher publisher(
            QueueRunner runner,
            LinuxBlockfsExtentResolver resolver
    ) {
        return new LinuxBlockfsExtentPublisher(
                new LinuxBlockfsControlCommand(Path.of("cxnfs-blockfs-ctl"), Path.of("/dev/test"), runner),
                resolver
        );
    }

    private LinuxBlockfsExtent resolveExtent(EarlyReadStoragePlan plan, EarlyReadPayloadExtent extent) {
        byte[] expectedHash = hash(40 + extent.chunkIndex()).bytes();
        return new LinuxBlockfsExtent(
                77,
                extent.fileOffset(),
                100 + extent.chunkIndex(),
                0,
                extent.length(),
                expectedHash
        );
    }

    private static EarlyReadStoragePlan plan(boolean requireTrailingBeforePublish) {
        FileManifest manifest = manifest("/datasets/train/shard.mds", 128 * 1024, 64 * 1024);
        return new EarlyReadStoragePlanner().plan(
                manifest,
                reconstructionPlan(manifest.fileHash()),
                0,
                128 * 1024,
                new EarlyReadStoragePolicy(
                        Path.of("/var/lib/cxnfs/disk-a"),
                        new ReadAheadPolicy(0, 128 * 1024),
                        CordPoolConfig.fixed(2),
                        true,
                        requireTrailingBeforePublish,
                        requireTrailingBeforePublish
                )
        );
    }

    private static FileManifest manifest(String path, int size, int chunkSize) {
        int chunks = size / chunkSize;
        return new FileManifest(
                path,
                hash(1),
                size,
                java.util.stream.IntStream.range(0, chunks)
                        .mapToObj(index -> new ChunkRef(hash(index + 2), chunkSize))
                        .toList()
        );
    }

    private static ReconstructionPlan reconstructionPlan(ContentHash fileHash) {
        return new ReconstructionPlan(
                fileHash,
                128L * 1024,
                List.of(
                        new ReconstructionTerm(hash(100), 0, 1),
                        new ReconstructionTerm(hash(101), 0, 1)
                )
        );
    }

    private static ContentHash hash(int seed) {
        byte[] bytes = new byte[ContentHash.BYTE_LENGTH];
        bytes[0] = (byte) seed;
        return ContentHash.of(bytes);
    }

    private static LinuxBlockfsControlCommand.CommandResult acceptedRegister() {
        return new LinuxBlockfsControlCommand.CommandResult(
                0,
                "registered_extent inode=77 file_offset=0 payload_block=100 payload_offset=0 payload_length=65536\n",
                ""
        );
    }

    private static LinuxBlockfsControlCommand.CommandResult unsupportedRegister() {
        return new LinuxBlockfsControlCommand.CommandResult(
                0,
                "unsupported=register-extent errno=95\n",
                ""
        );
    }

    private static LinuxBlockfsControlCommand.CommandResult registeredFile(long inodeNumber, String path) {
        return new LinuxBlockfsControlCommand.CommandResult(
                0,
                "registered_file inode=" + inodeNumber + " path=" + path + " size=4096 mode=444\n",
                ""
        );
    }

    private static final class QueueRunner implements LinuxBlockfsControlCommand.CommandRunner {
        private final Queue<LinuxBlockfsControlCommand.CommandResult> results = new ArrayDeque<>();
        private final List<List<String>> commands = new ArrayList<>();

        private QueueRunner(LinuxBlockfsControlCommand.CommandResult... results) {
            this.results.addAll(List.of(results));
        }

        @Override
        public LinuxBlockfsControlCommand.CommandResult run(List<String> command) {
            commands.add(command);
            if (results.isEmpty()) {
                throw new IllegalStateException("no command result queued");
            }
            return results.remove();
        }
    }
}
