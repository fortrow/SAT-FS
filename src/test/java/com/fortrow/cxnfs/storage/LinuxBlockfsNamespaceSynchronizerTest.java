package com.fortrow.cxnfs.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class LinuxBlockfsNamespaceSynchronizerTest {
    @TempDir
    Path tempDir;

    @Test
    void loadsMetadataRootNamespaceAndPublishesFiles() throws Exception {
        LinuxBlockfsNamespaceStore store = new LinuxBlockfsNamespaceStore();
        store.write(tempDir, new LinuxBlockfsNamespaceSnapshot(
                14,
                List.of(
                        LinuxBlockfsFile.readOnly(2, "datasets/train/sample.bin", 4096),
                        LinuxBlockfsFile.readOnly(3, "datasets/train/labels.mds", 8192)
                )
        ));
        QueueRunner runner = new QueueRunner(
                clearedNamespace(),
                registeredFile(2, "datasets/train/sample.bin"),
                registeredFile(3, "datasets/train/labels.mds")
        );
        LinuxBlockfsNamespaceSynchronizer synchronizer = new LinuxBlockfsNamespaceSynchronizer(
                store,
                new LinuxBlockfsManifestIndexStore(),
                new LinuxBlockfsExtentHintStore(),
                new LinuxBlockfsExtentPublisher(
                        new LinuxBlockfsControlCommand(Path.of("cxnfs-blockfs-ctl"), Path.of("/dev/test"), runner),
                        (plan, extent) -> {
                            throw new AssertionError("extent resolver should not be used for namespace sync");
                        }
                )
        );

        LinuxBlockfsNamespaceSyncResult result = synchronizer.sync(tempDir);

        assertEquals(tempDir, result.metadataRoot());
        assertEquals(14, result.generation());
        assertEquals(2, result.publishedFiles());
        assertEquals(0, result.manifestFiles());
        assertEquals(0, result.manifestChunks());
        assertEquals(0, result.attemptedExtents());
        assertEquals("clear-namespace", runner.commands.get(0).get(3));
        assertEquals("--metadata-root", runner.commands.get(0).get(4));
        assertEquals(tempDir.normalize().toString(), runner.commands.get(0).get(5));
        assertEquals("--metadata-root", runner.commands.get(1).get(4));
        assertEquals(tempDir.normalize().toString(), runner.commands.get(1).get(5));
        assertEquals("datasets/train/sample.bin", runner.commands.get(1).get(9));
        assertEquals("datasets/train/labels.mds", runner.commands.get(2).get(9));
    }

    @Test
    void loadsOptionalExtentHintsAndPublishesThemAfterFiles() throws Exception {
        LinuxBlockfsNamespaceStore namespaceStore = new LinuxBlockfsNamespaceStore();
        namespaceStore.write(tempDir, new LinuxBlockfsNamespaceSnapshot(
                14,
                List.of(LinuxBlockfsFile.readOnly(2, "datasets/train/sample.bin", 6144))
        ));
        LinuxBlockfsExtentHintStore extentHintStore = new LinuxBlockfsExtentHintStore();
        LinuxBlockfsManifestIndexStore manifestIndexStore = new LinuxBlockfsManifestIndexStore();
        manifestIndexStore.write(tempDir, new LinuxBlockfsManifestIndexSnapshot(
                14,
                List.of(new LinuxBlockfsManifestEntry(
                        2,
                        "datasets/train/sample.bin",
                        6144,
                        contentHash(20),
                        List.of(
                                new LinuxBlockfsManifestChunk(0, 4096, contentHash(1)),
                                new LinuxBlockfsManifestChunk(4096, 2048, contentHash(2))
                        )
                ))
        ));
        extentHintStore.write(tempDir, new LinuxBlockfsExtentHintsSnapshot(
                14,
                List.of(
                        new LinuxBlockfsExtent(2, 0, 1, 0, 4096, hash(1)),
                        new LinuxBlockfsExtent(2, 4096, 2, 0, 2048, hash(2))
                )
        ));
        QueueRunner runner = new QueueRunner(
                clearedNamespace(),
                registeredFile(2, "datasets/train/sample.bin"),
                registeredExtent(2, 0, 4096),
                registeredExtent(2, 4096, 2048)
        );
        LinuxBlockfsNamespaceSynchronizer synchronizer = new LinuxBlockfsNamespaceSynchronizer(
                namespaceStore,
                manifestIndexStore,
                extentHintStore,
                new LinuxBlockfsExtentPublisher(
                        new LinuxBlockfsControlCommand(Path.of("cxnfs-blockfs-ctl"), Path.of("/dev/test"), runner),
                        (plan, extent) -> {
                            throw new AssertionError("extent resolver should not be used for sidecar sync");
                        }
                )
        );

        LinuxBlockfsNamespaceSyncResult result = synchronizer.sync(tempDir);

        assertEquals(1, result.publishedFiles());
        assertEquals(1, result.manifestFiles());
        assertEquals(2, result.manifestChunks());
        assertEquals(2, result.auditedExtentHints());
        assertEquals(2, result.attemptedExtents());
        assertEquals(2, result.acceptedExtents());
        assertEquals(0, result.unsupportedExtents());
        assertEquals(6144, result.acceptedExtentBytes());
        assertEquals("clear-namespace", runner.commands.get(0).get(3));
        assertEquals("--metadata-root", runner.commands.get(0).get(4));
        assertEquals(tempDir.normalize().toString(), runner.commands.get(0).get(5));
        assertEquals("register-file", runner.commands.get(1).get(3));
        assertEquals("--metadata-root", runner.commands.get(1).get(4));
        assertEquals("register-extent", runner.commands.get(2).get(3));
        assertEquals("--metadata-root", runner.commands.get(2).get(4));
        assertEquals("register-extent", runner.commands.get(3).get(3));
        assertEquals("--metadata-root", runner.commands.get(3).get(4));
    }

    @Test
    void rejectsExtentHintsFromDifferentNamespaceGeneration() throws Exception {
        LinuxBlockfsNamespaceStore namespaceStore = new LinuxBlockfsNamespaceStore();
        namespaceStore.write(tempDir, new LinuxBlockfsNamespaceSnapshot(
                14,
                List.of(LinuxBlockfsFile.readOnly(2, "datasets/train/sample.bin", 4096))
        ));
        LinuxBlockfsExtentHintStore extentHintStore = new LinuxBlockfsExtentHintStore();
        extentHintStore.write(tempDir, new LinuxBlockfsExtentHintsSnapshot(
                15,
                List.of(new LinuxBlockfsExtent(2, 0, 1, 0, 4096, hash(1)))
        ));
        QueueRunner runner = new QueueRunner(registeredFile(2, "datasets/train/sample.bin"));
        LinuxBlockfsNamespaceSynchronizer synchronizer = new LinuxBlockfsNamespaceSynchronizer(
                namespaceStore,
                new LinuxBlockfsManifestIndexStore(),
                extentHintStore,
                new LinuxBlockfsExtentPublisher(
                        new LinuxBlockfsControlCommand(Path.of("cxnfs-blockfs-ctl"), Path.of("/dev/test"), runner),
                        (plan, extent) -> {
                            throw new AssertionError("extent resolver should not be used");
                        }
                )
        );

        assertThrows(IllegalStateException.class, () -> synchronizer.sync(tempDir));
        assertEquals(0, runner.commands.size());
    }

    @Test
    void rejectsExtentHintsWithoutManifestIndexAudit() throws Exception {
        LinuxBlockfsNamespaceStore namespaceStore = new LinuxBlockfsNamespaceStore();
        namespaceStore.write(tempDir, new LinuxBlockfsNamespaceSnapshot(
                14,
                List.of(LinuxBlockfsFile.readOnly(2, "datasets/train/sample.bin", 4096))
        ));
        LinuxBlockfsExtentHintStore extentHintStore = new LinuxBlockfsExtentHintStore();
        extentHintStore.write(tempDir, new LinuxBlockfsExtentHintsSnapshot(
                14,
                List.of(new LinuxBlockfsExtent(2, 0, 1, 0, 4096, hash(1)))
        ));
        QueueRunner runner = new QueueRunner();
        LinuxBlockfsNamespaceSynchronizer synchronizer = new LinuxBlockfsNamespaceSynchronizer(
                namespaceStore,
                new LinuxBlockfsManifestIndexStore(),
                extentHintStore,
                new LinuxBlockfsExtentPublisher(
                        new LinuxBlockfsControlCommand(Path.of("cxnfs-blockfs-ctl"), Path.of("/dev/test"), runner),
                        (plan, extent) -> {
                            throw new AssertionError("extent resolver should not be used");
                        }
                )
        );

        assertThrows(IllegalStateException.class, () -> synchronizer.sync(tempDir));
        assertEquals(0, runner.commands.size());
    }

    private static LinuxBlockfsControlCommand.CommandResult registeredFile(long inodeNumber, String path) {
        return new LinuxBlockfsControlCommand.CommandResult(
                0,
                "registered_file inode=" + inodeNumber + " path=" + path + " size=4096 mode=444\n",
                ""
        );
    }

    private static LinuxBlockfsControlCommand.CommandResult clearedNamespace() {
        return new LinuxBlockfsControlCommand.CommandResult(
                0,
                "cleared_namespace=true\n",
                ""
        );
    }

    private static LinuxBlockfsControlCommand.CommandResult registeredExtent(
            long inodeNumber,
            long offset,
            int length
    ) {
        return new LinuxBlockfsControlCommand.CommandResult(
                0,
                "registered_extent inode=" + inodeNumber + " file_offset=" + offset
                        + " payload_block=1 payload_offset=0 payload_length=" + length + "\n",
                ""
        );
    }

    private static byte[] hash(int seed) {
        byte[] hash = new byte[32];
        hash[0] = (byte) seed;
        return hash;
    }

    private static com.fortrow.cxnfs.hash.ContentHash contentHash(int seed) {
        return com.fortrow.cxnfs.hash.ContentHash.of(hash(seed));
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
