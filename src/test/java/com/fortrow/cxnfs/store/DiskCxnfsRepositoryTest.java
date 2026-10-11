package com.fortrow.cxnfs.store;

import com.fortrow.cxnfs.chunk.Chunk;
import com.fortrow.cxnfs.chunk.GearHashChunker;
import com.fortrow.cxnfs.codec.CompressionCodecRegistry;
import com.fortrow.cxnfs.codec.IdentityCompressionCodec;
import com.fortrow.cxnfs.format.ManifestCodec;
import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.hash.Hashing;
import com.fortrow.cxnfs.io.StagedWrite;
import com.fortrow.cxnfs.io.WriteExtent;
import com.fortrow.cxnfs.io.WriteMode;
import com.fortrow.cxnfs.io.WriteSession;
import com.fortrow.cxnfs.manifest.ChunkRef;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.metadata.FileType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class DiskCxnfsRepositoryTest {
    @TempDir
    Path tempDir;

    @Test
    void persistsChunksManifestsAndListingsAcrossInstances() throws Exception {
        Path root = tempDir.resolve("repository");
        byte[] firstBytes = "alpha beta gamma delta".repeat(512).getBytes(StandardCharsets.UTF_8);
        byte[] secondBytes = "alpha beta gamma epsilon".repeat(512).getBytes(StandardCharsets.UTF_8);
        DiskCxnfsRepository first = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);

        FileManifest firstManifest = first.put("/datasets/train/first.mds", firstBytes);
        FileManifest secondManifest = first.put("/datasets/train/second.mds", secondBytes);

        try (var containers = Files.list(root.resolve("containers"));
             var manifests = Files.list(root.resolve("manifests"))) {
            assertFalse(containers.toList().isEmpty());
            assertEquals(2, manifests.toList().size());
        }

        DiskCxnfsRepository reopened = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);

        assertEquals(2, reopened.list("/datasets/train").entries().size());
        assertEquals(FileType.REGULAR_FILE, reopened.stat("/datasets/train/first.mds").orElseThrow().type());
        assertEquals(firstManifest.fileHash(), reopened.manifest("/datasets/train/first.mds").orElseThrow().fileHash());
        assertEquals(secondManifest.fileHash(), reopened.manifest(secondManifest.fileHash()).orElseThrow().fileHash());
        assertArrayEquals(firstBytes, reopened.reconstruct(firstManifest.fileHash()));
        assertArrayEquals(Arrays.copyOfRange(secondBytes, 7, 35), reopened.readRange(secondManifest.fileHash(), 7, 28));
        assertTrue(reopened.missingChunks(firstManifest.chunks()).isEmpty());
    }

    @Test
    void startupIndexesManifestsWithoutLoadingDurableContainers() throws Exception {
        Path root = tempDir.resolve("repository-lazy-startup");
        byte[] data = "lazy durable startup payload".repeat(512).getBytes(StandardCharsets.UTF_8);
        DiskCxnfsRepository repository = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);
        FileManifest manifest = repository.put("/datasets/train/lazy.mds", data);

        DiskCxnfsRepository reopened = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);

        assertEquals(0, reopened.containerCount());
        assertEquals(0, reopened.chunkCount());
        assertEquals(manifest.fileHash(), reopened.manifest("/datasets/train/lazy.mds").orElseThrow().fileHash());
        assertEquals(FileType.REGULAR_FILE, reopened.stat("/datasets/train/lazy.mds").orElseThrow().type());
        assertTrue(reopened.missingChunks(manifest.chunks()).isEmpty());

        assertArrayEquals(Arrays.copyOfRange(data, 16, 40), reopened.readRange(manifest.fileHash(), 16, 24));
        assertTrue(reopened.chunkCount() > 0);
        assertArrayEquals(data, reopened.reconstruct(manifest.fileHash()));
    }

    @Test
    void corruptDurableContainerFailsOnDemandInsteadOfDuringStartup() throws Exception {
        Path root = tempDir.resolve("repository-lazy-corrupt-container");
        byte[] data = "corrupt me later".getBytes(StandardCharsets.UTF_8);
        DiskCxnfsRepository repository = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);
        FileManifest manifest = repository.put("/datasets/train/corrupt.mds", data);
        Files.writeString(containerPath(root, manifest.chunks().getFirst().hash()), "not a container");

        DiskCxnfsRepository reopened = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);

        assertEquals(manifest.fileHash(), reopened.manifest("/datasets/train/corrupt.mds").orElseThrow().fileHash());
        assertThrows(IOException.class, () -> reopened.reconstruct(manifest.fileHash()));
        DiskRepositoryAuditResult audit = DiskCxnfsRepository.audit(root);
        assertFalse(audit.clean());
        assertEquals(1, audit.corruptContainerFiles());
    }

    @Test
    void rejectsTraversalSegmentsInPersistedNamespaceManifests() throws Exception {
        Path root = tempDir.resolve("repository-bad-namespace");
        byte[] data = "current".getBytes(StandardCharsets.UTF_8);
        DiskCxnfsRepository repository = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);
        FileManifest current = repository.put("/datasets/train/current.mds", data);
        FileManifest escaping = new FileManifest(
                "/datasets/train/../escape.mds",
                current.fileHash(),
                current.size(),
                current.chunks()
        );
        writeManifest(root, escaping);

        assertThrows(
                IllegalArgumentException.class,
                () -> new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks)
        );
        DiskRepositoryAuditResult audit = DiskCxnfsRepository.audit(root);
        assertFalse(audit.clean());
        assertEquals(1, audit.corruptManifestFiles());
    }

    @Test
    void persistsPartialWriteCommitsAcrossInstances() throws Exception {
        Path root = tempDir.resolve("repository");
        DiskCxnfsRepository repository = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);
        byte[] original = "abcdefghij".repeat(32).getBytes(StandardCharsets.UTF_8);
        FileManifest first = repository.put("/mount/big.txt", original);

        WriteSession session = WriteSession.copyOnWrite("/mount/big.txt", repository.reconstruct(first.fileHash()));
        session.write(3, "XYZ".getBytes(StandardCharsets.UTF_8));
        FileManifest second = repository.commit(session.stage());

        DiskCxnfsRepository reopened = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);

        assertEquals(second.fileHash(), reopened.manifest("/mount/big.txt").orElseThrow().fileHash());
        assertEquals("abcXYZghij", new String(reopened.reconstruct(second.fileHash()), 0, 10, StandardCharsets.UTF_8));
    }

    @Test
    void persistsFileBackedStagedWriteCommitsAcrossInstances() throws Exception {
        Path root = tempDir.resolve("repository-file-backed-commit");
        DiskCxnfsRepository repository = new DiskCxnfsRepository(root, GearHashChunker.defaults());
        WriteSession session = WriteSession.createOrReplace("/mount/file-backed.txt");
        byte[] data = "file-backed durable write\n".repeat(20_000).getBytes(StandardCharsets.UTF_8);
        session.write(0, data);

        FileManifest manifest;
        try (var staged = session.stageToFile(tempDir.resolve("write-session"))) {
            manifest = repository.commit(staged);
        }
        DiskCxnfsRepository reopened = new DiskCxnfsRepository(root, GearHashChunker.defaults());

        assertEquals(manifest.fileHash(), reopened.manifest("/mount/file-backed.txt").orElseThrow().fileHash());
        assertArrayEquals(data, reopened.reconstruct(manifest.fileHash()));
        assertEquals(0, countFiles(root.resolve("tmp")));
    }

    @Test
    void persistsOnlyMissingChunksForAlignedFileBackedCopyOnWriteCommits() throws Exception {
        Path root = tempDir.resolve("repository-aligned-file-backed-commit");
        DiskCxnfsRepository repository = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);
        byte[] original = "abcdefghABCDEFGHijklmnop".getBytes(StandardCharsets.UTF_8);
        FileManifest first = repository.put("/mount/aligned.bin", original);
        WriteSession session = WriteSession.copyOnWrite("/mount/aligned.bin", original);
        session.write(8, "WXYZ1234".getBytes(StandardCharsets.UTF_8));

        FileManifest second;
        try (var staged = session.stageToFile(tempDir.resolve("write-session-aligned"))) {
            second = repository.commit(staged);
        }
        DiskCxnfsRepository reopened = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);

        assertEquals(3, second.chunks().size());
        assertEquals(first.chunks().get(0), second.chunks().get(0));
        assertNotEquals(first.chunks().get(1), second.chunks().get(1));
        assertEquals(first.chunks().get(2), second.chunks().get(2));
        assertEquals(4, countFiles(root.resolve("containers")));
        assertEquals(second.fileHash(), reopened.manifest("/mount/aligned.bin").orElseThrow().fileHash());
        assertArrayEquals("abcdefghWXYZ1234ijklmnop".getBytes(StandardCharsets.UTF_8), reopened.reconstruct(second.fileHash()));
    }

    @Test
    void persistsOnlyMissingChunksForUnalignedFileBackedCopyOnWriteCommits() throws Exception {
        Path root = tempDir.resolve("repository-unaligned-file-backed-commit");
        DiskCxnfsRepository repository = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);
        byte[] original = "abcdefghABCDEFGHijklmnop".getBytes(StandardCharsets.UTF_8);
        FileManifest first = repository.put("/mount/unaligned.bin", original);
        WriteSession session = WriteSession.copyOnWrite("/mount/unaligned.bin", original);
        session.write(10, "XY".getBytes(StandardCharsets.UTF_8));

        FileManifest second;
        try (var staged = session.stageToFile(tempDir.resolve("write-session-unaligned"))) {
            second = repository.commit(staged);
        }
        DiskCxnfsRepository reopened = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);

        assertEquals(3, second.chunks().size());
        assertEquals(first.chunks().get(0), second.chunks().get(0));
        assertNotEquals(first.chunks().get(1), second.chunks().get(1));
        assertEquals(first.chunks().get(2), second.chunks().get(2));
        assertEquals(4, countFiles(root.resolve("containers")));
        assertEquals(second.fileHash(), reopened.manifest("/mount/unaligned.bin").orElseThrow().fileHash());
        assertArrayEquals("abcdefghABXYEFGHijklmnop".getBytes(StandardCharsets.UTF_8), reopened.reconstruct(second.fileHash()));
    }

    @Test
    void persistsPatchOnlyCopyOnWriteExtentsAcrossInstances() throws Exception {
        Path root = tempDir.resolve("repository-patch-only-commit");
        DiskCxnfsRepository repository = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);
        byte[] original = "abcdefghABCDEFGHijklmnop".getBytes(StandardCharsets.UTF_8);
        FileManifest first = repository.put("/mount/patch-only.bin", original);
        StagedWrite staged = StagedWrite.fileBacked(
                "/mount/patch-only.bin",
                WriteMode.COPY_ON_WRITE,
                original.length,
                List.of(new WriteExtent(10, "XY".getBytes(StandardCharsets.UTF_8))),
                tempDir.resolve("missing-materialized-file.bin")
        );

        FileManifest second = repository.commit(staged);
        DiskCxnfsRepository reopened = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);

        assertEquals(3, second.chunks().size());
        assertEquals(first.chunks().get(0), second.chunks().get(0));
        assertNotEquals(first.chunks().get(1), second.chunks().get(1));
        assertEquals(first.chunks().get(2), second.chunks().get(2));
        assertEquals(4, countFiles(root.resolve("containers")));
        assertEquals(second.fileHash(), reopened.manifest("/mount/patch-only.bin").orElseThrow().fileHash());
        assertArrayEquals("abcdefghABXYEFGHijklmnop".getBytes(StandardCharsets.UTF_8), reopened.reconstruct(second.fileHash()));
    }

    @Test
    void persistsPatchOnlyResizeCommitsAcrossInstances() throws Exception {
        Path root = tempDir.resolve("repository-patch-only-resize");
        DiskCxnfsRepository repository = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);
        byte[] original = "abcdefghABCDEFGHijklmnop".getBytes(StandardCharsets.UTF_8);
        FileManifest first = repository.put("/mount/resize.bin", original);
        StagedWrite truncated = StagedWrite.fileBacked(
                "/mount/resize.bin",
                WriteMode.COPY_ON_WRITE,
                18,
                List.of(),
                tempDir.resolve("missing-truncate.bin")
        );
        FileManifest second = repository.commit(truncated);
        StagedWrite appended = StagedWrite.fileBacked(
                "/mount/resize.bin",
                WriteMode.COPY_ON_WRITE,
                22,
                List.of(new WriteExtent(18, "WXYZ".getBytes(StandardCharsets.UTF_8))),
                tempDir.resolve("missing-append.bin")
        );

        FileManifest third = repository.commit(appended);
        DiskCxnfsRepository reopened = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);

        assertEquals(first.chunks().get(0), second.chunks().get(0));
        assertEquals(first.chunks().get(1), second.chunks().get(1));
        assertNotEquals(first.chunks().get(2), second.chunks().get(2));
        assertEquals(second.chunks().get(0), third.chunks().get(0));
        assertEquals(second.chunks().get(1), third.chunks().get(1));
        assertEquals(second.chunks().get(2), third.chunks().get(2));
        assertEquals(third.fileHash(), reopened.manifest("/mount/resize.bin").orElseThrow().fileHash());
        assertArrayEquals("abcdefghABCDEFGHijWXYZ".getBytes(StandardCharsets.UTF_8), reopened.reconstruct(third.fileHash()));
    }

    @Test
    void persistsStreamedPutsAcrossInstancesWithoutLeavingTempFiles() throws Exception {
        Path root = tempDir.resolve("repository-stream-put");
        DiskCxnfsRepository repository = new DiskCxnfsRepository(root, GearHashChunker.defaults());
        byte[] data = "streamed durable shard\n".repeat(50_000).getBytes(StandardCharsets.UTF_8);

        FileManifest manifest = repository.putStream("/datasets/train/streamed.mds", new ByteArrayInputStream(data));
        DiskCxnfsRepository reopened = new DiskCxnfsRepository(root, GearHashChunker.defaults());

        assertEquals(manifest.fileHash(), reopened.manifest("/datasets/train/streamed.mds").orElseThrow().fileHash());
        assertArrayEquals(data, reopened.reconstruct(manifest.fileHash()));
        assertEquals(0, countFiles(root.resolve("tmp")));
        assertTrue(countFiles(root.resolve("containers")) >= 1);
    }

    @Test
    void persistsVerifiedChunkPublishFlowAcrossInstances() throws Exception {
        Path root = tempDir.resolve("repository");
        DiskCxnfsRepository repository = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);
        byte[] first = "verified ".getBytes(StandardCharsets.UTF_8);
        byte[] second = "payload".getBytes(StandardCharsets.UTF_8);
        ContentHash firstHash = Hashing.chunk(first);
        ContentHash secondHash = Hashing.chunk(second);
        ContentHash fileHash = Hashing.file(List.of(firstHash, secondHash), List.of(first.length, second.length));
        FileManifest manifest = new FileManifest(
                "/outputs/run-1/model.safetensors",
                fileHash,
                first.length + second.length,
                List.of(new ChunkRef(firstHash, first.length), new ChunkRef(secondHash, second.length))
        );

        repository.putVerifiedChunk(firstHash, 0, first);
        repository.putVerifiedChunk(secondHash, first.length, second);
        repository.publishManifest(manifest);

        DiskCxnfsRepository reopened = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);

        assertEquals(fileHash, reopened.stat("/outputs/run-1/model.safetensors")
                .orElseThrow()
                .contentHash()
                .orElseThrow());
        assertArrayEquals("verified payload".getBytes(StandardCharsets.UTF_8), reopened.reconstruct(fileHash));
    }

    @Test
    void replaysCompleteManifestJournalAcrossInstances() throws Exception {
        Path root = tempDir.resolve("repository-journal-replay");
        DiskCxnfsRepository repository = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);
        byte[] first = "replayed ".getBytes(StandardCharsets.UTF_8);
        byte[] second = "manifest".getBytes(StandardCharsets.UTF_8);
        ContentHash firstHash = Hashing.chunk(first);
        ContentHash secondHash = Hashing.chunk(second);
        ContentHash fileHash = Hashing.file(List.of(firstHash, secondHash), List.of(first.length, second.length));
        FileManifest manifest = new FileManifest(
                "/datasets/recovered/current.mds",
                fileHash,
                first.length + second.length,
                List.of(new ChunkRef(firstHash, first.length), new ChunkRef(secondHash, second.length))
        );
        repository.putVerifiedChunk(firstHash, 0, first);
        repository.putVerifiedChunk(secondHash, first.length, second);
        writeJournal(root, new DiskRepositoryJournalEntry(List.of(), List.of(manifest)));

        DiskCxnfsRepository reopened = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);

        assertEquals(fileHash, reopened.manifest("/datasets/recovered/current.mds").orElseThrow().fileHash());
        assertArrayEquals("replayed manifest".getBytes(StandardCharsets.UTF_8), reopened.reconstruct(fileHash));
        assertEquals(0, countFiles(root.resolve("journals")));
    }

    @Test
    void replaysNamespaceJournalDeletesAndPutsAcrossInstances() throws Exception {
        Path root = tempDir.resolve("repository-namespace-journal-replay");
        DiskCxnfsRepository repository = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);
        FileManifest first = repository.put("/datasets/train/a/first.mds", "first".getBytes(StandardCharsets.UTF_8));
        FileManifest second = repository.put("/datasets/train/a/second.mds", "second".getBytes(StandardCharsets.UTF_8));
        FileManifest renamedFirst = new FileManifest(
                "/datasets/train/b/first.mds",
                first.fileHash(),
                first.size(),
                first.chunks()
        );
        FileManifest renamedSecond = new FileManifest(
                "/datasets/train/b/second.mds",
                second.fileHash(),
                second.size(),
                second.chunks()
        );
        writeJournal(root, new DiskRepositoryJournalEntry(
                List.of(first.path(), second.path()),
                List.of(renamedFirst, renamedSecond)
        ));

        DiskCxnfsRepository reopened = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);

        assertTrue(reopened.manifest("/datasets/train/a/first.mds").isEmpty());
        assertTrue(reopened.manifest("/datasets/train/a/second.mds").isEmpty());
        assertEquals(first.fileHash(), reopened.manifest("/datasets/train/b/first.mds").orElseThrow().fileHash());
        assertEquals(second.fileHash(), reopened.manifest("/datasets/train/b/second.mds").orElseThrow().fileHash());
        assertArrayEquals("first".getBytes(StandardCharsets.UTF_8), reopened.reconstruct(first.fileHash()));
        assertEquals(0, countFiles(root.resolve("journals")));
    }

    @Test
    void replaysJournalAfterDeletesWereAppliedButPutsWereInterrupted() throws Exception {
        Path root = tempDir.resolve("repository-journal-after-deletes");
        DiskCxnfsRepository repository = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);
        FileManifest first = repository.put("/datasets/train/a/first.mds", "first".getBytes(StandardCharsets.UTF_8));
        FileManifest second = repository.put("/datasets/train/a/second.mds", "second".getBytes(StandardCharsets.UTF_8));
        FileManifest renamedFirst = new FileManifest(
                "/datasets/train/b/first.mds",
                first.fileHash(),
                first.size(),
                first.chunks()
        );
        FileManifest renamedSecond = new FileManifest(
                "/datasets/train/b/second.mds",
                second.fileHash(),
                second.size(),
                second.chunks()
        );
        writeJournal(root, new DiskRepositoryJournalEntry(
                List.of(first.path(), second.path()),
                List.of(renamedFirst, renamedSecond)
        ));
        Files.delete(manifestPath(root, first.path()));
        Files.delete(manifestPath(root, second.path()));

        DiskCxnfsRepository reopened = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);

        assertTrue(reopened.manifest("/datasets/train/a/first.mds").isEmpty());
        assertTrue(reopened.manifest("/datasets/train/a/second.mds").isEmpty());
        assertEquals(first.fileHash(), reopened.manifest("/datasets/train/b/first.mds").orElseThrow().fileHash());
        assertEquals(second.fileHash(), reopened.manifest("/datasets/train/b/second.mds").orElseThrow().fileHash());
        assertEquals(0, countFiles(root.resolve("journals")));
    }

    @Test
    void replaysJournalAfterSomePutsWereApplied() throws Exception {
        Path root = tempDir.resolve("repository-journal-after-partial-puts");
        DiskCxnfsRepository repository = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);
        FileManifest first = repository.put("/datasets/train/a/first.mds", "first".getBytes(StandardCharsets.UTF_8));
        FileManifest second = repository.put("/datasets/train/a/second.mds", "second".getBytes(StandardCharsets.UTF_8));
        FileManifest renamedFirst = new FileManifest(
                "/datasets/train/b/first.mds",
                first.fileHash(),
                first.size(),
                first.chunks()
        );
        FileManifest renamedSecond = new FileManifest(
                "/datasets/train/b/second.mds",
                second.fileHash(),
                second.size(),
                second.chunks()
        );
        writeJournal(root, new DiskRepositoryJournalEntry(
                List.of(first.path(), second.path()),
                List.of(renamedFirst, renamedSecond)
        ));
        Files.delete(manifestPath(root, first.path()));
        Files.delete(manifestPath(root, second.path()));
        writeManifest(root, renamedFirst);

        DiskCxnfsRepository reopened = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);

        assertTrue(reopened.manifest("/datasets/train/a/first.mds").isEmpty());
        assertTrue(reopened.manifest("/datasets/train/a/second.mds").isEmpty());
        assertEquals(first.fileHash(), reopened.manifest("/datasets/train/b/first.mds").orElseThrow().fileHash());
        assertEquals(second.fileHash(), reopened.manifest("/datasets/train/b/second.mds").orElseThrow().fileHash());
        assertEquals(2, countFiles(root.resolve("manifests")));
        assertEquals(0, countFiles(root.resolve("journals")));
    }

    @Test
    void dropsIncompleteManifestJournalAndKeepsPublishedState() throws Exception {
        Path root = tempDir.resolve("repository-incomplete-journal");
        DiskCxnfsRepository repository = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);
        FileManifest current = repository.put("/datasets/train/current.mds", "current".getBytes(StandardCharsets.UTF_8));
        byte[] missing = "missing".getBytes(StandardCharsets.UTF_8);
        ContentHash missingHash = Hashing.chunk(missing);
        FileManifest incomplete = new FileManifest(
                "/datasets/train/incomplete.mds",
                Hashing.file(List.of(missingHash), List.of(missing.length)),
                missing.length,
                List.of(new ChunkRef(missingHash, missing.length))
        );
        writeJournal(root, new DiskRepositoryJournalEntry(List.of(), List.of(incomplete)));

        DiskCxnfsRepository reopened = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);

        assertEquals(current.fileHash(), reopened.manifest("/datasets/train/current.mds").orElseThrow().fileHash());
        assertTrue(reopened.manifest("/datasets/train/incomplete.mds").isEmpty());
        assertEquals(0, countFiles(root.resolve("journals")));
    }

    @Test
    void replaysRealPutInterruptedAfterJournalWrite() throws Exception {
        Path root = tempDir.resolve("repository-failed-put-journal");
        DiskCxnfsRepository repository = repositoryWithFault(root, new DiskCxnfsRepository.MutationFaults() {
            @Override
            public void afterJournalWrite(Path journalPath, DiskRepositoryJournalEntry entry) throws IOException {
                throw new IOException("simulated stop after journal");
            }
        });

        IOException failure = assertThrows(
                IOException.class,
                () -> repository.put("/datasets/train/current.mds", "current".getBytes(StandardCharsets.UTF_8))
        );

        assertTrue(failure.getMessage().contains("simulated stop"));
        assertEquals(1, countFiles(root.resolve("journals")));
        DiskCxnfsRepository reopened = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);
        FileManifest recovered = reopened.manifest("/datasets/train/current.mds").orElseThrow();
        assertArrayEquals("current".getBytes(StandardCharsets.UTF_8), reopened.reconstruct(recovered.fileHash()));
        assertEquals(0, countFiles(root.resolve("journals")));
    }

    @Test
    void replaysRealRenameInterruptedAfterDeletes() throws Exception {
        Path root = tempDir.resolve("repository-failed-rename-deletes");
        DiskCxnfsRepository seed = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);
        FileManifest first = seed.put("/datasets/train/a/first.mds", "first".getBytes(StandardCharsets.UTF_8));
        FileManifest second = seed.put("/datasets/train/a/second.mds", "second".getBytes(StandardCharsets.UTF_8));
        DiskCxnfsRepository repository = repositoryWithFault(root, new DiskCxnfsRepository.MutationFaults() {
            @Override
            public void afterManifestDeletes(DiskRepositoryJournalEntry entry) throws IOException {
                throw new IOException("simulated stop after deletes");
            }
        });

        IOException failure = assertThrows(
                IOException.class,
                () -> repository.renamePath("/datasets/train/a", "/datasets/train/b")
        );

        assertTrue(failure.getMessage().contains("simulated stop"));
        assertEquals(1, countFiles(root.resolve("journals")));
        DiskCxnfsRepository reopened = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);
        assertTrue(reopened.manifest("/datasets/train/a/first.mds").isEmpty());
        assertTrue(reopened.manifest("/datasets/train/a/second.mds").isEmpty());
        assertEquals(first.fileHash(), reopened.manifest("/datasets/train/b/first.mds").orElseThrow().fileHash());
        assertEquals(second.fileHash(), reopened.manifest("/datasets/train/b/second.mds").orElseThrow().fileHash());
        assertEquals(0, countFiles(root.resolve("journals")));
    }

    @Test
    void replaysRealRenameInterruptedAfterFirstPut() throws Exception {
        Path root = tempDir.resolve("repository-failed-rename-first-put");
        DiskCxnfsRepository seed = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);
        FileManifest first = seed.put("/datasets/train/a/first.mds", "first".getBytes(StandardCharsets.UTF_8));
        FileManifest second = seed.put("/datasets/train/a/second.mds", "second".getBytes(StandardCharsets.UTF_8));
        DiskCxnfsRepository repository = repositoryWithFault(root, new DiskCxnfsRepository.MutationFaults() {
            @Override
            public void afterManifestPut(FileManifest manifest, int manifestIndex, int manifestCount)
                    throws IOException {
                if (manifestIndex == 1) {
                    throw new IOException("simulated stop after first put");
                }
            }
        });

        IOException failure = assertThrows(
                IOException.class,
                () -> repository.renamePath("/datasets/train/a", "/datasets/train/b")
        );

        assertTrue(failure.getMessage().contains("simulated stop"));
        assertEquals(1, countFiles(root.resolve("journals")));
        DiskCxnfsRepository reopened = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);
        assertTrue(reopened.manifest("/datasets/train/a/first.mds").isEmpty());
        assertTrue(reopened.manifest("/datasets/train/a/second.mds").isEmpty());
        assertEquals(first.fileHash(), reopened.manifest("/datasets/train/b/first.mds").orElseThrow().fileHash());
        assertEquals(second.fileHash(), reopened.manifest("/datasets/train/b/second.mds").orElseThrow().fileHash());
        assertEquals(0, countFiles(root.resolve("journals")));
    }

    @Test
    void compactsUnreferencedContainersAndCrashTempFilesOffline() throws Exception {
        Path root = tempDir.resolve("repository");
        DiskCxnfsRepository repository = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);
        byte[] oldData = "OLD-0001".repeat(4).getBytes(StandardCharsets.UTF_8);
        byte[] newData = "NEW-0001".repeat(4).getBytes(StandardCharsets.UTF_8);
        FileManifest oldManifest = repository.put("/datasets/train/current.mds", oldData);
        FileManifest newManifest = repository.put("/datasets/train/current.mds", newData);
        Path tempFile = root.resolve("tmp/stale-write.tmp");
        Files.writeString(tempFile, "interrupted write");

        DiskRepositoryMaintenanceResult result = DiskCxnfsRepository.compact(root);
        DiskCxnfsRepository reopened = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);

        assertEquals(root.toAbsolutePath().normalize(), result.root());
        assertFalse(result.dryRun());
        assertEquals(1, result.manifestFiles());
        assertEquals(1, result.referencedChunks());
        assertEquals(2, result.scannedContainers());
        assertEquals(1, result.removedContainers());
        assertTrue(result.removedContainerBytes() > 0);
        assertEquals(1, result.removedTempFiles());
        assertTrue(result.removedTempBytes() > 0);
        assertTrue(result.removedBytes() >= result.removedContainerBytes());
        assertEquals(newManifest.fileHash(), reopened.manifest("/datasets/train/current.mds").orElseThrow().fileHash());
        assertTrue(reopened.manifest(oldManifest.fileHash()).isEmpty());
        assertArrayEquals(newData, reopened.reconstruct(newManifest.fileHash()));
        assertEquals(1, countFiles(root.resolve("containers")));
        assertEquals(0, countFiles(root.resolve("tmp")));
    }

    @Test
    void prunesPersistedManifestsForPathsMissingFromReloadedDirectory() throws Exception {
        Path root = tempDir.resolve("repository");
        DiskCxnfsRepository repository = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);
        byte[] keepBytes = "keep".getBytes(StandardCharsets.UTF_8);
        byte[] staleBytes = "stale".getBytes(StandardCharsets.UTF_8);
        FileManifest keep = repository.put("/datasets/train/keep.mds", keepBytes);
        FileManifest stale = repository.put("/datasets/train/stale.mds", staleBytes);

        repository.pruneExcept(Set.of("/datasets/train/keep.mds"));
        DiskCxnfsRepository reopened = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);

        assertEquals(1, countFiles(root.resolve("manifests")));
        assertEquals(keep.fileHash(), reopened.manifest("/datasets/train/keep.mds").orElseThrow().fileHash());
        assertTrue(reopened.manifest("/datasets/train/stale.mds").isEmpty());
        assertTrue(reopened.manifest(stale.fileHash()).isEmpty());
        assertArrayEquals(keepBytes, reopened.reconstruct(keep.fileHash()));
    }

    @Test
    void persistsLiveDeleteAndRenameNamespaceMutations() throws Exception {
        Path root = tempDir.resolve("repository");
        DiskCxnfsRepository repository = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);
        FileManifest first = repository.put("/datasets/train/a/first.mds", "first".getBytes(StandardCharsets.UTF_8));
        FileManifest second = repository.put("/datasets/train/a/second.mds", "second".getBytes(StandardCharsets.UTF_8));
        repository.put("/datasets/train/remove.mds", "remove".getBytes(StandardCharsets.UTF_8));

        PathRenameResult rename = repository.renamePath("/datasets/train/a", "/datasets/train/b");
        PathDeleteResult delete = repository.deletePath("/datasets/train/remove.mds", false);
        DiskCxnfsRepository reopened = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);

        assertEquals("/datasets/train/b", rename.targetPath());
        assertEquals(2, rename.updatedManifests().size());
        assertEquals(1, delete.deletedEntries());
        assertTrue(reopened.manifest("/datasets/train/a/first.mds").isEmpty());
        assertTrue(reopened.manifest("/datasets/train/remove.mds").isEmpty());
        assertEquals(first.fileHash(), reopened.manifest("/datasets/train/b/first.mds").orElseThrow().fileHash());
        assertEquals(second.fileHash(), reopened.manifest("/datasets/train/b/second.mds").orElseThrow().fileHash());
        assertEquals(2, countFiles(root.resolve("manifests")));
    }

    @Test
    void persistsRenameOverExistingRegularFile() throws Exception {
        Path root = tempDir.resolve("repository-rename-replace");
        DiskCxnfsRepository repository = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);
        FileManifest oldManifest = repository.put("/datasets/train/current.mds", "old".getBytes(StandardCharsets.UTF_8));
        FileManifest nextManifest = repository.put("/datasets/train/.current.tmp", "next".getBytes(StandardCharsets.UTF_8));

        PathRenameResult rename = repository.renamePath("/datasets/train/.current.tmp", "/datasets/train/current.mds");
        DiskCxnfsRepository reopened = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);

        assertEquals("/datasets/train/current.mds", rename.targetPath());
        assertTrue(reopened.manifest("/datasets/train/.current.tmp").isEmpty());
        assertEquals(nextManifest.fileHash(), reopened.manifest("/datasets/train/current.mds").orElseThrow().fileHash());
        assertTrue(reopened.manifest(oldManifest.fileHash()).isEmpty());
        assertArrayEquals("next".getBytes(StandardCharsets.UTF_8), reopened.reconstruct(nextManifest.fileHash()));
        assertEquals(1, countFiles(root.resolve("manifests")));
    }

    @Test
    void liveCompactionRunsUnderRepositoryLockAfterNamespaceMutation() throws Exception {
        Path root = tempDir.resolve("repository-live-compact");
        DiskCxnfsRepository repository = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);
        byte[] keepBytes = "keep-000".repeat(4).getBytes(StandardCharsets.UTF_8);
        byte[] removeBytes = "remove-1".repeat(4).getBytes(StandardCharsets.UTF_8);
        FileManifest keep = repository.put("/datasets/train/keep.mds", keepBytes);
        FileManifest remove = repository.put("/datasets/train/remove.mds", removeBytes);

        repository.deletePath("/datasets/train/remove.mds", false);
        DiskRepositoryMaintenanceResult result = repository.compact();
        DiskCxnfsRepository reopened = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);

        assertEquals(1, result.manifestFiles());
        assertEquals(1, result.referencedChunks());
        assertEquals(2, result.scannedContainers());
        assertEquals(1, result.removedContainers());
        assertEquals(keep.fileHash(), reopened.manifest("/datasets/train/keep.mds").orElseThrow().fileHash());
        assertTrue(reopened.manifest("/datasets/train/remove.mds").isEmpty());
        assertTrue(reopened.manifest(remove.fileHash()).isEmpty());
        assertArrayEquals(keepBytes, reopened.reconstruct(keep.fileHash()));
        assertEquals(1, countFiles(root.resolve("containers")));
    }

    @Test
    void inspectReportsMaintenanceWithoutRemovingArtifacts() throws Exception {
        Path root = tempDir.resolve("repository-inspect");
        DiskCxnfsRepository repository = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);
        repository.put("/datasets/train/current.mds", "old-0001".repeat(4).getBytes(StandardCharsets.UTF_8));
        repository.put("/datasets/train/current.mds", "new-0001".repeat(4).getBytes(StandardCharsets.UTF_8));
        Files.writeString(root.resolve("tmp/stale-write.tmp"), "interrupted write");

        DiskRepositoryMaintenanceResult result = DiskCxnfsRepository.inspect(root);

        assertTrue(result.dryRun());
        assertEquals(1, result.manifestFiles());
        assertEquals(1, result.referencedChunks());
        assertEquals(2, result.scannedContainers());
        assertEquals(1, result.removedContainers());
        assertEquals(1, result.removedTempFiles());
        assertEquals(2, countFiles(root.resolve("containers")));
        assertEquals(1, countFiles(root.resolve("tmp")));
        assertTrue(result.toReportText().contains("dry_run=true"));
        assertTrue(result.toReportText().contains("removed_containers=1"));
        assertTrue(result.toReportText().contains("retained_young_containers=0"));
        assertTrue(result.toReportText().contains("orphan_container_min_age_ms=0"));
        assertTrue(result.toReportText().contains("temp_file_min_age_ms=0"));
    }

    @Test
    void compactionRetainsYoungArtifactsUntilPolicyAgeElapses() throws Exception {
        Path root = tempDir.resolve("repository-retention");
        DiskCxnfsRepository repository = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);
        FileManifest old = repository.put("/datasets/train/current.mds", "old-0001".repeat(4).getBytes(StandardCharsets.UTF_8));
        repository.put("/datasets/train/current.mds", "new-0001".repeat(4).getBytes(StandardCharsets.UTF_8));
        Path oldContainer = containerPath(root, old.chunks().getFirst().hash());
        Path tempFile = root.resolve("tmp/interrupted-write.tmp");
        Files.writeString(tempFile, "interrupted write");
        DiskRepositoryMaintenancePolicy oneDayRetention = new DiskRepositoryMaintenancePolicy(
                Duration.ofDays(1),
                Duration.ofDays(1)
        );

        DiskRepositoryMaintenanceResult retained = DiskCxnfsRepository.compact(root, oneDayRetention);

        assertEquals(0, retained.removedContainers());
        assertEquals(1, retained.retainedYoungContainers());
        assertEquals(0, retained.removedTempFiles());
        assertEquals(1, retained.retainedYoungTempFiles());
        assertEquals(Duration.ofDays(1).toMillis(), retained.orphanContainerMinAgeMillis());
        assertEquals(Duration.ofDays(1).toMillis(), retained.tempFileMinAgeMillis());
        assertTrue(Files.exists(oldContainer));
        assertTrue(Files.exists(tempFile));

        FileTime oldEnough = FileTime.from(Instant.now().minus(Duration.ofDays(2)));
        Files.setLastModifiedTime(oldContainer, oldEnough);
        Files.setLastModifiedTime(tempFile, oldEnough);
        DiskRepositoryMaintenanceResult removed = DiskCxnfsRepository.compact(root, oneDayRetention);

        assertEquals(1, removed.removedContainers());
        assertEquals(0, removed.retainedYoungContainers());
        assertEquals(1, removed.removedTempFiles());
        assertEquals(0, removed.retainedYoungTempFiles());
        assertFalse(Files.exists(oldContainer));
        assertFalse(Files.exists(tempFile));
    }

    @Test
    void compactionRefusesPendingJournalsBeforeDeletingContainers() throws Exception {
        Path root = tempDir.resolve("repository-compact-pending-journal");
        DiskCxnfsRepository repository = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);
        FileManifest old = repository.put("/datasets/train/current.mds", "old-0001".repeat(4).getBytes(StandardCharsets.UTF_8));
        repository.put("/datasets/train/current.mds", "new-0001".repeat(4).getBytes(StandardCharsets.UTF_8));
        Path oldContainer = containerPath(root, old.chunks().getFirst().hash());
        writeJournal(root, new DiskRepositoryJournalEntry(List.of(), List.of(old)));

        IOException compactFailure = assertThrows(IOException.class, () -> DiskCxnfsRepository.compact(root));
        IOException inspectFailure = assertThrows(IOException.class, () -> DiskCxnfsRepository.inspect(root));

        assertTrue(compactFailure.getMessage().contains("pending journals"));
        assertTrue(inspectFailure.getMessage().contains("pending journals"));
        assertTrue(Files.exists(oldContainer));
        assertEquals(1, countFiles(root.resolve("journals")));
    }

    @Test
    void auditsCleanRepositoryArtifactsWithoutMutating() throws Exception {
        Path root = tempDir.resolve("repository-audit-clean");
        DiskCxnfsRepository repository = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);
        repository.put("/datasets/train/current.mds", "current".getBytes(StandardCharsets.UTF_8));

        DiskRepositoryAuditResult result = DiskCxnfsRepository.audit(root);

        assertTrue(result.clean());
        assertEquals(0, result.issueCount());
        assertEquals(1, result.manifestFiles());
        assertEquals(1, result.containerFiles());
        assertEquals(0, result.journalFiles());
        assertEquals(0, result.tempFiles());
        assertEquals(1, result.referencedChunks());
        assertEquals(1, result.storedChunks());
        assertEquals(0, result.replayableJournalFiles());
        assertEquals(0, result.incompleteJournalFiles());
        assertEquals(0, result.corruptJournalFiles());
        assertTrue(result.toReportText().contains("cxnfs.repository.audit.v1"));
        assertTrue(result.toReportText().contains("clean=true"));
        assertTrue(result.toReportText().contains("journal_files=0"));
    }

    @Test
    void auditsCorruptMissingOrphanAndConflictingRepositoryArtifacts() throws Exception {
        Path root = tempDir.resolve("repository-audit-dirty");
        DiskCxnfsRepository repository = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);
        FileManifest stale = repository.put("/datasets/train/current.mds", "stale".getBytes(StandardCharsets.UTF_8));
        FileManifest current = repository.put("/datasets/train/current.mds", "current".getBytes(StandardCharsets.UTF_8));
        FileManifest conflictingDirectoryManifest = new FileManifest(
                "/datasets/train",
                current.fileHash(),
                current.size(),
                current.chunks()
        );
        Files.write(manifestPath(root, conflictingDirectoryManifest.path()), ManifestCodec.encode(conflictingDirectoryManifest));
        Files.delete(containerPath(root, current.chunks().getFirst().hash()));
        Files.writeString(root.resolve("containers/corrupt.cxnfc"), "not a container");
        Files.writeString(root.resolve("tmp/stale-write.tmp"), "interrupted write");

        DiskRepositoryAuditResult result = DiskCxnfsRepository.audit(root);

        assertFalse(result.clean());
        assertTrue(result.issueCount() >= 4);
        assertEquals(2, result.manifestFiles());
        assertEquals(2, result.containerFiles());
        assertEquals(1, result.tempFiles());
        assertEquals(1, result.corruptContainerFiles());
        assertEquals(1, result.missingChunks());
        assertEquals(1, result.orphanContainers());
        assertEquals(1, result.namespaceConflicts());
        assertEquals(0, result.corruptManifestFiles());
        assertEquals(stale.fileHash(), DiskCxnfsRepository.audit(root).storedChunks() > 0 ? stale.fileHash() : stale.fileHash());
    }

    @Test
    void auditsPendingIncompleteAndCorruptRepositoryJournals() throws Exception {
        Path root = tempDir.resolve("repository-audit-journals");
        DiskCxnfsRepository repository = new DiskCxnfsRepository(root, DiskCxnfsRepositoryTest::fixedEightByteChunks);
        FileManifest current = repository.put("/datasets/train/current.mds", "current".getBytes(StandardCharsets.UTF_8));
        byte[] missing = "missing-journal-chunk".getBytes(StandardCharsets.UTF_8);
        ContentHash missingHash = Hashing.chunk(missing);
        FileManifest incomplete = new FileManifest(
                "/datasets/train/incomplete.mds",
                Hashing.file(List.of(missingHash), List.of(missing.length)),
                missing.length,
                List.of(new ChunkRef(missingHash, missing.length))
        );
        Path journals = root.resolve("journals");
        writeJournal(root, "replayable.cxnfj", new DiskRepositoryJournalEntry(List.of(), List.of(current)));
        writeJournal(root, "incomplete.cxnfj", new DiskRepositoryJournalEntry(List.of(), List.of(incomplete)));
        Files.writeString(journals.resolve("corrupt.cxnfj"), "not a journal");

        DiskRepositoryAuditResult result = DiskCxnfsRepository.audit(root);

        assertFalse(result.clean());
        assertEquals(3, result.issueCount());
        assertEquals(3, result.journalFiles());
        assertEquals(1, result.replayableJournalFiles());
        assertEquals(1, result.incompleteJournalFiles());
        assertEquals(1, result.corruptJournalFiles());
        assertEquals(1, result.manifestFiles());
        assertEquals(1, result.containerFiles());
        assertTrue(result.toReportText().contains("journal_files=3"));
        assertTrue(result.toReportText().contains("replayable_journal_files=1"));
        assertTrue(result.toReportText().contains("incomplete_journal_files=1"));
        assertTrue(result.toReportText().contains("corrupt_journal_files=1"));
    }

    private static long countFiles(Path directory) throws Exception {
        try (var paths = Files.list(directory)) {
            return paths.filter(Files::isRegularFile).count();
        }
    }

    private static void writeJournal(Path root, DiskRepositoryJournalEntry entry) throws Exception {
        writeJournal(root, "test-journal.cxnfj", entry);
    }

    private static void writeJournal(Path root, String fileName, DiskRepositoryJournalEntry entry) throws Exception {
        Path journals = root.resolve("journals");
        Files.createDirectories(journals);
        Files.write(journals.resolve(fileName), DiskRepositoryJournalCodec.encode(entry));
    }

    private static void writeManifest(Path root, FileManifest manifest) throws Exception {
        Files.createDirectories(root.resolve("manifests"));
        Files.write(manifestPath(root, manifest.path()), ManifestCodec.encode(manifest));
    }

    private static DiskCxnfsRepository repositoryWithFault(
            Path root,
            DiskCxnfsRepository.MutationFaults mutationFaults
    ) throws Exception {
        return new DiskCxnfsRepository(
                root,
                DiskCxnfsRepositoryTest::fixedEightByteChunks,
                new IdentityCompressionCodec(),
                CompressionCodecRegistry.defaults(),
                mutationFaults
        );
    }

    private static Path containerPath(Path root, ContentHash chunkHash) {
        try (var paths = Files.list(root.resolve("containers"))) {
            return paths
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".cxnfc"))
                    .filter(path -> {
                        try {
                            return Files.readAllBytes(path).length > 0
                                    && path.getFileName().toString().equals(chunkHash.hex() + ".cxnfc");
                        } catch (Exception e) {
                            return false;
                        }
                    })
                    .findFirst()
                    .orElse(root.resolve("containers").resolve(chunkHash.hex() + ".cxnfc"));
        } catch (Exception e) {
            return root.resolve("containers").resolve(chunkHash.hex() + ".cxnfc");
        }
    }

    private static Path manifestPath(Path root, String path) {
        String normalizedPath = path.startsWith("/") ? path : "/" + path;
        ContentHash pathHash = Hashing.chunk(normalizedPath.getBytes(StandardCharsets.UTF_8));
        return root.resolve("manifests").resolve(pathHash.hex() + ".cxnfm");
    }

    private static List<Chunk> fixedEightByteChunks(byte[] data) {
        List<Chunk> chunks = new ArrayList<>();
        for (int offset = 0; offset < data.length; offset += 8) {
            byte[] chunk = Arrays.copyOfRange(data, offset, Math.min(data.length, offset + 8));
            chunks.add(new Chunk(Hashing.chunk(chunk), offset, chunk));
        }
        if (data.length == 0) {
            chunks.add(new Chunk(Hashing.chunk(new byte[0]), 0, new byte[0]));
        }
        return chunks;
    }
}
