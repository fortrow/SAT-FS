package com.fortrow.cxnfs.store;

import com.fortrow.cxnfs.chunk.Chunk;
import com.fortrow.cxnfs.chunk.GearHashChunker;
import com.fortrow.cxnfs.codec.CompressionCodec;
import com.fortrow.cxnfs.codec.CompressionCodecRegistry;
import com.fortrow.cxnfs.codec.Lz4FrameCompressionCodec;
import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.hash.Hashing;
import com.fortrow.cxnfs.io.ReadPlan;
import com.fortrow.cxnfs.io.StagedWrite;
import com.fortrow.cxnfs.io.WriteExtent;
import com.fortrow.cxnfs.io.WriteMode;
import com.fortrow.cxnfs.io.WriteSession;
import com.fortrow.cxnfs.manifest.ChunkRef;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.metadata.DirectoryListing;
import com.fortrow.cxnfs.metadata.FileMetadata;
import com.fortrow.cxnfs.metadata.FileType;
import com.fortrow.cxnfs.reconstruction.ReconstructionPlan;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class CxnfsRepositoryTest {
    @TempDir
    Path tempDir;

    @Test
    void storesDeduplicatesPlansAndReconstructsFiles() throws Exception {
        byte[] firstBytes = repeated("alpha beta gamma\n", 20_000);
        byte[] secondBytes = (repeatedString("alpha beta gamma\n", 10_000)
                + "delta\n"
                + repeatedString("alpha beta gamma\n", 10_000)).getBytes(StandardCharsets.UTF_8);

        CxnfsRepository repository = new CxnfsRepository(GearHashChunker.defaults());
        FileManifest first = repository.put("/data/first.txt", firstBytes);
        FileManifest second = repository.put("/data/second.txt", secondBytes);

        assertNotEquals(first.fileHash(), second.fileHash());
        assertArrayEquals(firstBytes, repository.reconstruct(first.fileHash()));
        assertArrayEquals(secondBytes, repository.reconstruct(second.fileHash()));
        assertTrue(repository.chunkCount() < first.chunks().size() + second.chunks().size());

        ReconstructionPlan plan = repository.plan(second.fileHash());
        assertEquals(second.fileHash(), plan.fileHash());
        assertEquals(secondBytes.length, plan.fileSize());
        assertTrue(plan.terms().size() >= 1);
    }

    @Test
    void streamsPutsThroughTheChunkerAndReconstructsContent() throws Exception {
        byte[] data = repeated("streamed dataset shard\n", 50_000);
        CxnfsRepository repository = new CxnfsRepository(GearHashChunker.defaults());

        FileManifest streamed = repository.putStream("/data/streamed.mds", new ByteArrayInputStream(data));
        FileManifest ordinary = repository.put("/data/ordinary.mds", data);

        assertEquals(ordinary.fileHash(), streamed.fileHash());
        assertEquals(ordinary.size(), streamed.size());
        assertEquals(ordinary.chunks(), streamed.chunks());
        assertArrayEquals(data, repository.reconstruct(streamed.fileHash()));
        assertTrue(repository.chunkCount() <= streamed.chunks().size());
    }

    @Test
    void listsDirectoriesWithoutFetchingFileContents() throws Exception {
        CxnfsRepository repository = new CxnfsRepository(GearHashChunker.defaults());
        FileManifest first = repository.put("data//nested/first.txt", "first".getBytes(StandardCharsets.UTF_8));
        repository.put("/data/second.txt", "second".getBytes(StandardCharsets.UTF_8));

        DirectoryListing root = repository.list("/");
        assertEquals("/", root.path());
        assertEquals(1, root.entries().size());
        assertEntry(root.entries().getFirst(), "/data", "data", FileType.DIRECTORY, 0);

        DirectoryListing data = repository.list("/data");
        assertEquals(2, data.entries().size());
        assertEntry(data.entries().getFirst(), "/data/nested", "nested", FileType.DIRECTORY, 0);
        assertEntry(data.entries().get(1), "/data/second.txt", "second.txt", FileType.REGULAR_FILE, 6);

        DirectoryListing nested = repository.list("/data/nested");
        assertEquals(1, nested.entries().size());
        assertEquals(first.fileHash(), nested.entries().getFirst().contentHash().orElseThrow());
    }

    @Test
    void rejectsTraversalSegmentsAcrossNamespaceOperations() throws Exception {
        CxnfsRepository repository = new CxnfsRepository(GearHashChunker.defaults());
        byte[] data = "payload".getBytes(StandardCharsets.UTF_8);
        ContentHash chunkHash = Hashing.chunk(data);
        ContentHash fileHash = Hashing.file(List.of(chunkHash), List.of(data.length));

        repository.put("/datasets/train/current.mds", data);
        repository.putVerifiedChunk(chunkHash, 0, data);

        assertThrows(IllegalArgumentException.class, () -> repository.put("/datasets/../escape.mds", data));
        assertThrows(
                IllegalArgumentException.class,
                () -> repository.putStream("/datasets/./escape.mds", new ByteArrayInputStream(data))
        );
        assertThrows(IllegalArgumentException.class, () -> repository.list("/datasets/../train"));
        assertThrows(IllegalArgumentException.class, () -> repository.stat("/datasets/./train"));
        assertThrows(IllegalArgumentException.class, () -> repository.manifest("/datasets/../escape.mds"));
        assertThrows(IllegalArgumentException.class, () -> repository.deletePath("/datasets/../train", true));
        assertThrows(
                IllegalArgumentException.class,
                () -> repository.renamePath("/datasets/train/current.mds", "/datasets/../renamed.mds")
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> repository.publishManifest(new FileManifest(
                        "/datasets/../published.mds",
                        fileHash,
                        data.length,
                        List.of(new ChunkRef(chunkHash, data.length))
                ))
        );
    }

    @Test
    void statsFilesAndSyntheticDirectories() throws Exception {
        CxnfsRepository repository = new CxnfsRepository(GearHashChunker.defaults());
        FileManifest manifest = repository.put("/docs/readme.txt", "hello".getBytes(StandardCharsets.UTF_8));

        Optional<FileMetadata> root = repository.stat("/");
        Optional<FileMetadata> docs = repository.stat("/docs");
        Optional<FileMetadata> file = repository.stat("/docs/readme.txt");

        assertTrue(root.isPresent());
        assertEquals(FileType.DIRECTORY, root.orElseThrow().type());
        assertTrue(docs.isPresent());
        assertEquals(FileType.DIRECTORY, docs.orElseThrow().type());
        assertTrue(file.isPresent());
        assertEquals(FileType.REGULAR_FILE, file.orElseThrow().type());
        assertEquals(manifest.fileHash(), file.orElseThrow().contentHash().orElseThrow());
        assertTrue(repository.stat("/missing").isEmpty());
    }

    @Test
    void plansReadsAndCommitsStagedWrites() throws Exception {
        CxnfsRepository repository = new CxnfsRepository(GearHashChunker.defaults());
        FileManifest first = repository.put("/mount/big.txt", repeated("abcdefghij", 20_000));

        ReadPlan readPlan = repository.readPlan(first.fileHash(), 5, 100);
        byte[] range = repository.readRange(first.fileHash(), 5, 100);

        assertEquals(first, repository.manifest("/mount/big.txt").orElseThrow());
        assertEquals(first, repository.manifest(first.fileHash()).orElseThrow());
        assertEquals(5, readPlan.requestedRange().offset());
        assertEquals(100, readPlan.requestedRange().length());
        assertTrue(readPlan.chunks().stream().mapToInt(chunk -> chunk.bytesToRead()).sum() >= 100);
        assertArrayEquals(Arrays.copyOfRange(repository.reconstruct(first.fileHash()), 5, 105), range);

        WriteSession session = WriteSession.copyOnWrite("/mount/big.txt", repository.reconstruct(first.fileHash()));
        session.write(3, "XYZ".getBytes(StandardCharsets.UTF_8));
        FileManifest second = repository.commit(session.stage());

        assertEquals("/mount/big.txt", second.path());
        assertEquals("abcXYZghij", new String(repository.reconstruct(second.fileHash()), 0, 10, StandardCharsets.UTF_8));
    }

    @Test
    void commitsFileBackedStagedWritesThroughStreamingPath() throws Exception {
        CxnfsRepository repository = new CxnfsRepository(GearHashChunker.defaults());
        WriteSession session = WriteSession.createOrReplace("/mount/file-backed.txt");
        session.write(0, repeated("file-backed staged write\n", 20_000));

        FileManifest manifest;
        try (var staged = session.stageToFile(tempDir)) {
            manifest = repository.commit(staged);
        }

        assertEquals("/mount/file-backed.txt", manifest.path());
        assertEquals("file-backed staged write\n", new String(
                repository.readRange(manifest.fileHash(), 0, "file-backed staged write\n".length()),
                StandardCharsets.UTF_8
        ));
    }

    @Test
    void commitsChunkAlignedCopyOnWriteByPreservingUnchangedManifestChunks() throws Exception {
        CxnfsRepository repository = new CxnfsRepository(CxnfsRepositoryTest::fixedFourByteChunks);
        byte[] original = "abcdefghijkl".getBytes(StandardCharsets.UTF_8);
        FileManifest first = repository.put("/mount/aligned.bin", original);
        WriteSession session = WriteSession.copyOnWrite("/mount/aligned.bin", original);
        session.write(4, "WXYZ".getBytes(StandardCharsets.UTF_8));

        FileManifest second;
        try (var staged = session.stageToFile(tempDir)) {
            second = repository.commit(staged);
        }

        assertEquals(3, second.chunks().size());
        assertEquals(first.chunks().get(0), second.chunks().get(0));
        assertNotEquals(first.chunks().get(1), second.chunks().get(1));
        assertEquals(first.chunks().get(2), second.chunks().get(2));
        assertEquals(4, repository.chunkCount());
        assertArrayEquals("abcdWXYZijkl".getBytes(StandardCharsets.UTF_8), repository.reconstruct(second.fileHash()));
    }

    @Test
    void commitsUnalignedCopyOnWriteByPatchingOnlyOverlappingChunks() throws Exception {
        CxnfsRepository repository = new CxnfsRepository(CxnfsRepositoryTest::fixedFourByteChunks);
        byte[] original = "abcdefghijkl".getBytes(StandardCharsets.UTF_8);
        FileManifest first = repository.put("/mount/unaligned.bin", original);
        WriteSession session = WriteSession.copyOnWrite("/mount/unaligned.bin", original);
        session.write(6, "WXYZ".getBytes(StandardCharsets.UTF_8));

        FileManifest second;
        try (var staged = session.stageToFile(tempDir)) {
            second = repository.commit(staged);
        }

        assertEquals(3, first.chunks().size());
        assertEquals(3, second.chunks().size());
        assertEquals(first.chunks().get(0), second.chunks().get(0));
        assertNotEquals(first.chunks().get(1), second.chunks().get(1));
        assertNotEquals(first.chunks().get(2), second.chunks().get(2));
        assertEquals(5, repository.chunkCount());
        assertArrayEquals("abcdefWXYZkl".getBytes(StandardCharsets.UTF_8), repository.reconstruct(second.fileHash()));
    }

    @Test
    void commitsPatchOnlyCopyOnWriteFromExtentsWithoutMaterializedFile() throws Exception {
        CxnfsRepository repository = new CxnfsRepository(CxnfsRepositoryTest::fixedFourByteChunks);
        byte[] original = "abcdefghijkl".getBytes(StandardCharsets.UTF_8);
        FileManifest first = repository.put("/mount/patch-only.bin", original);
        StagedWrite staged = StagedWrite.fileBacked(
                "/mount/patch-only.bin",
                WriteMode.COPY_ON_WRITE,
                original.length,
                List.of(new WriteExtent(5, "XY".getBytes(StandardCharsets.UTF_8))),
                tempDir.resolve("missing-materialized-file.bin")
        );

        FileManifest second = repository.commit(staged);

        assertEquals(3, second.chunks().size());
        assertEquals(first.chunks().get(0), second.chunks().get(0));
        assertNotEquals(first.chunks().get(1), second.chunks().get(1));
        assertEquals(first.chunks().get(2), second.chunks().get(2));
        assertArrayEquals("abcdeXYhijkl".getBytes(StandardCharsets.UTF_8), repository.reconstruct(second.fileHash()));
    }

    @Test
    void commitsSeparatePatchOnlyExtentsWithoutRewritingCleanMiddleChunks() throws Exception {
        CxnfsRepository repository = new CxnfsRepository(CxnfsRepositoryTest::fixedFourByteChunks);
        byte[] original = "abcdefghijklmnop".getBytes(StandardCharsets.UTF_8);
        FileManifest first = repository.put("/mount/separate-patches.bin", original);
        StagedWrite staged = StagedWrite.fileBacked(
                "/mount/separate-patches.bin",
                WriteMode.COPY_ON_WRITE,
                original.length,
                List.of(
                        new WriteExtent(5, "XY".getBytes(StandardCharsets.UTF_8)),
                        new WriteExtent(13, "UV".getBytes(StandardCharsets.UTF_8))
                ),
                tempDir.resolve("missing-separate-patches.bin")
        );

        FileManifest second = repository.commit(staged);

        assertEquals(4, second.chunks().size());
        assertEquals(first.chunks().get(0), second.chunks().get(0));
        assertNotEquals(first.chunks().get(1), second.chunks().get(1));
        assertEquals(first.chunks().get(2), second.chunks().get(2));
        assertNotEquals(first.chunks().get(3), second.chunks().get(3));
        assertEquals(6, repository.chunkCount());
        assertArrayEquals("abcdeXYhijklmUVp".getBytes(StandardCharsets.UTF_8), repository.reconstruct(second.fileHash()));
    }

    @Test
    void commitsPatchOnlyTruncationWithoutMaterializedFile() throws Exception {
        CxnfsRepository repository = new CxnfsRepository(CxnfsRepositoryTest::fixedFourByteChunks);
        byte[] original = "abcdefghijkl".getBytes(StandardCharsets.UTF_8);
        FileManifest first = repository.put("/mount/truncate.bin", original);
        StagedWrite staged = StagedWrite.fileBacked(
                "/mount/truncate.bin",
                WriteMode.COPY_ON_WRITE,
                10,
                List.of(),
                tempDir.resolve("missing-truncate.bin")
        );

        FileManifest second = repository.commit(staged);

        assertEquals(3, first.chunks().size());
        assertEquals(3, second.chunks().size());
        assertEquals(first.chunks().get(0), second.chunks().get(0));
        assertEquals(first.chunks().get(1), second.chunks().get(1));
        assertNotEquals(first.chunks().get(2), second.chunks().get(2));
        assertArrayEquals("abcdefghij".getBytes(StandardCharsets.UTF_8), repository.reconstruct(second.fileHash()));
    }

    @Test
    void commitsPatchOnlyAppendWithoutMaterializedFile() throws Exception {
        CxnfsRepository repository = new CxnfsRepository(CxnfsRepositoryTest::fixedFourByteChunks);
        byte[] original = "abcdefgh".getBytes(StandardCharsets.UTF_8);
        FileManifest first = repository.put("/mount/append.bin", original);
        StagedWrite staged = StagedWrite.fileBacked(
                "/mount/append.bin",
                WriteMode.COPY_ON_WRITE,
                12,
                List.of(new WriteExtent(8, "WXYZ".getBytes(StandardCharsets.UTF_8))),
                tempDir.resolve("missing-append.bin")
        );

        FileManifest second = repository.commit(staged);

        assertEquals(3, second.chunks().size());
        assertEquals(first.chunks().get(0), second.chunks().get(0));
        assertEquals(first.chunks().get(1), second.chunks().get(1));
        assertArrayEquals("abcdefghWXYZ".getBytes(StandardCharsets.UTF_8), repository.reconstruct(second.fileHash()));
    }

    @Test
    void commitsPatchOnlySparseGrowthWithZeroFill() throws Exception {
        CxnfsRepository repository = new CxnfsRepository(CxnfsRepositoryTest::fixedFourByteChunks);
        byte[] original = "abcdefgh".getBytes(StandardCharsets.UTF_8);
        repository.put("/mount/sparse.bin", original);
        StagedWrite staged = StagedWrite.fileBacked(
                "/mount/sparse.bin",
                WriteMode.COPY_ON_WRITE,
                12,
                List.of(new WriteExtent(10, "YZ".getBytes(StandardCharsets.UTF_8))),
                tempDir.resolve("missing-sparse-growth.bin")
        );

        FileManifest manifest = repository.commit(staged);

        assertArrayEquals(new byte[]{
                'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 0, 0, 'Y', 'Z'
        }, repository.reconstruct(manifest.fileHash()));
    }

    @Test
    void commitsPatchOnlyNoopExtentPastDeclaredSizeWithoutMaterializedFile() throws Exception {
        CxnfsRepository repository = new CxnfsRepository(CxnfsRepositoryTest::fixedFourByteChunks);
        byte[] original = "abcdefgh".getBytes(StandardCharsets.UTF_8);
        FileManifest first = repository.put("/mount/noop.bin", original);
        StagedWrite staged = StagedWrite.fileBacked(
                "/mount/noop.bin",
                WriteMode.COPY_ON_WRITE,
                original.length,
                List.of(new WriteExtent(12, "ignored".getBytes(StandardCharsets.UTF_8))),
                tempDir.resolve("missing-noop.bin")
        );

        FileManifest second = repository.commit(staged);

        assertEquals(first, second);
        assertArrayEquals(original, repository.reconstruct(second.fileHash()));
    }

    @Test
    void reconstructsLz4CompressedContainers() throws Exception {
        byte[] data = repeated("training shard block\n", 30_000);
        CxnfsRepository repository = new CxnfsRepository(GearHashChunker.defaults(), new Lz4FrameCompressionCodec());

        FileManifest manifest = repository.put("/dataset/shard.bin", data);

        assertArrayEquals(data, repository.reconstruct(manifest.fileHash()));
    }

    @Test
    void rangeReadsOnlyDecompressTouchedChunks() throws Exception {
        CountingCodec codec = new CountingCodec();
        CxnfsRepository repository = new CxnfsRepository(
                CxnfsRepositoryTest::fixedFourByteChunks,
                codec,
                new CompressionCodecRegistry(List.of(codec))
        );
        byte[] data = "abcdefghijklmnopqrst".getBytes(StandardCharsets.UTF_8);
        FileManifest manifest = repository.put("/dataset/shard.mds", data);

        byte[] range = repository.readRange(manifest.fileHash(), 6, 5);

        assertArrayEquals("ghijk".getBytes(StandardCharsets.UTF_8), range);
        assertEquals(2, codec.decompressions());
    }

    @Test
    void publishesVerifiedChunksAndManifestWithoutWholeFilePut() throws Exception {
        CxnfsRepository repository = new CxnfsRepository(GearHashChunker.defaults());
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
        FileManifest published = repository.publishManifest(manifest);

        assertEquals(manifest, published);
        assertEquals(fileHash, repository.stat("/outputs/run-1/model.safetensors")
                .orElseThrow()
                .contentHash()
                .orElseThrow());
        assertArrayEquals("verified payload".getBytes(StandardCharsets.UTF_8), repository.reconstruct(fileHash));
    }

    @Test
    void prunesStaleNamespacePathsWithoutRemovingSharedContent() throws Exception {
        byte[] data = "shared payload".getBytes(StandardCharsets.UTF_8);
        CxnfsRepository repository = new CxnfsRepository(GearHashChunker.defaults());
        FileManifest first = repository.put("/datasets/train/first.mds", data);
        FileManifest second = repository.put("/datasets/train/second.mds", data);

        repository.pruneExcept(Set.of("/datasets/train/second.mds"));

        assertTrue(repository.manifest("/datasets/train/first.mds").isEmpty());
        assertEquals(second.fileHash(), repository.manifest("/datasets/train/second.mds").orElseThrow().fileHash());
        assertEquals(1, repository.list("/datasets/train").entries().size());
        assertArrayEquals(data, repository.reconstruct(first.fileHash()));
    }

    @Test
    void deletesFilesAndRecursiveDirectoriesWithoutRemovingSharedContent() throws Exception {
        byte[] shared = "shared payload".getBytes(StandardCharsets.UTF_8);
        CxnfsRepository repository = new CxnfsRepository(GearHashChunker.defaults());
        FileManifest first = repository.put("/datasets/train/first.mds", shared);
        FileManifest second = repository.put("/datasets/train/second.mds", shared);
        repository.put("/datasets/eval/eval.mds", "eval".getBytes(StandardCharsets.UTF_8));

        PathDeleteResult fileDelete = repository.deletePath("/datasets/train/first.mds", false);
        assertEquals("/datasets/train/first.mds", fileDelete.path());
        assertEquals(1, fileDelete.deletedEntries());
        assertTrue(repository.manifest("/datasets/train/first.mds").isEmpty());
        assertEquals(second.fileHash(), repository.manifest(first.fileHash()).orElseThrow().fileHash());
        assertArrayEquals(shared, repository.reconstruct(first.fileHash()));

        assertThrows(IllegalStateException.class, () -> repository.deletePath("/datasets/train", false));
        PathDeleteResult directoryDelete = repository.deletePath("/datasets/train", true);

        assertEquals(1, directoryDelete.deletedEntries());
        assertTrue(repository.stat("/datasets/train").isEmpty());
        assertTrue(repository.manifest(second.fileHash()).isEmpty());
        assertFalse(repository.list("/datasets").entries().isEmpty());
    }

    @Test
    void renamesFilesAndDirectoryTrees() throws Exception {
        CxnfsRepository repository = new CxnfsRepository(GearHashChunker.defaults());
        byte[] firstBytes = "first".getBytes(StandardCharsets.UTF_8);
        byte[] secondBytes = "second".getBytes(StandardCharsets.UTF_8);
        FileManifest first = repository.put("/datasets/train/a/first.mds", firstBytes);
        FileManifest second = repository.put("/datasets/train/a/second.mds", secondBytes);

        PathRenameResult fileRename = repository.renamePath(
                "/datasets/train/a/first.mds",
                "/datasets/train/a/renamed.mds"
        );
        assertEquals("/datasets/train/a/renamed.mds", fileRename.targetPath());
        assertEquals("/datasets/train/a/renamed.mds", fileRename.metadata().path());
        assertTrue(repository.manifest("/datasets/train/a/first.mds").isEmpty());
        assertEquals(first.fileHash(), repository.manifest("/datasets/train/a/renamed.mds").orElseThrow().fileHash());
        assertArrayEquals(firstBytes, repository.reconstruct(first.fileHash()));

        PathRenameResult directoryRename = repository.renamePath("/datasets/train/a", "/datasets/train/b");

        assertEquals(2, directoryRename.updatedManifests().size());
        assertTrue(repository.stat("/datasets/train/a").isEmpty());
        assertEquals(first.fileHash(), repository.manifest("/datasets/train/b/renamed.mds").orElseThrow().fileHash());
        assertEquals(second.fileHash(), repository.manifest("/datasets/train/b/second.mds").orElseThrow().fileHash());
        assertThrows(IllegalStateException.class, () -> repository.renamePath("/datasets/train/b", "/datasets/train/b/nested"));

        PathRenameResult replacementRename = repository.renamePath(
                "/datasets/train/b/second.mds",
                "/datasets/train/b/renamed.mds"
        );
        assertEquals("/datasets/train/b/renamed.mds", replacementRename.targetPath());
        assertTrue(repository.manifest("/datasets/train/b/second.mds").isEmpty());
        assertEquals(second.fileHash(), repository.manifest("/datasets/train/b/renamed.mds").orElseThrow().fileHash());
        assertTrue(repository.manifest(first.fileHash()).isEmpty());
    }

    @Test
    void rejectsWritesThatWouldCreateImpossibleNamespaceTrees() throws Exception {
        CxnfsRepository repository = new CxnfsRepository(GearHashChunker.defaults());
        repository.put("/datasets/file", "file".getBytes(StandardCharsets.UTF_8));
        repository.put("/datasets/tree/child.mds", "child".getBytes(StandardCharsets.UTF_8));

        assertThrows(
                IllegalStateException.class,
                () -> repository.put("/datasets/file/child.mds", "bad".getBytes(StandardCharsets.UTF_8))
        );
        assertThrows(
                IllegalStateException.class,
                () -> repository.put("/datasets/tree", "bad".getBytes(StandardCharsets.UTF_8))
        );
    }

    @Test
    void rejectsManifestPublishThatWouldCreateImpossibleNamespaceTrees() throws Exception {
        CxnfsRepository repository = new CxnfsRepository(GearHashChunker.defaults());
        repository.put("/outputs/file", "file".getBytes(StandardCharsets.UTF_8));
        byte[] data = "payload".getBytes(StandardCharsets.UTF_8);
        ContentHash chunkHash = Hashing.chunk(data);
        ContentHash fileHash = Hashing.file(List.of(chunkHash), List.of(data.length));

        repository.putVerifiedChunk(chunkHash, 0, data);

        assertThrows(
                IllegalStateException.class,
                () -> repository.publishManifest(new FileManifest(
                        "/outputs/file/model.safetensors",
                        fileHash,
                        data.length,
                        List.of(new ChunkRef(chunkHash, data.length))
                ))
        );
    }

    @Test
    void keepsRepositoryStateConsistentDuringConcurrentReadsAndNamespaceMutations() throws Exception {
        CxnfsRepository repository = new CxnfsRepository(CxnfsRepositoryTest::fixedFourByteChunks);
        byte[] stableBytes = "stable training shard payload".repeat(32).getBytes(StandardCharsets.UTF_8);
        FileManifest stable = repository.put("/datasets/train/stable.mds", stableBytes);
        repository.put("/scratch/current.mds", "initial".getBytes(StandardCharsets.UTF_8));

        List<Callable<Void>> tasks = new ArrayList<>();
        for (int reader = 0; reader < 6; reader++) {
            tasks.add(() -> {
                for (int iteration = 0; iteration < 100; iteration++) {
                    assertArrayEquals(stableBytes, repository.reconstruct(stable.fileHash()));
                    assertArrayEquals(Arrays.copyOfRange(stableBytes, 3, 19), repository.readRange(stable.fileHash(), 3, 16));
                    assertEquals(stable.fileHash(), repository.stat("/datasets/train/stable.mds")
                            .orElseThrow()
                            .contentHash()
                            .orElseThrow());
                    assertTrue(repository.list("/datasets/train").entries().stream()
                            .anyMatch(entry -> entry.path().equals("/datasets/train/stable.mds")));
                }
                return null;
            });
        }
        tasks.add(() -> {
            for (int iteration = 0; iteration < 50; iteration++) {
                String next = "/scratch/file-" + iteration + ".mds";
                repository.put(next, ("payload-" + iteration).getBytes(StandardCharsets.UTF_8));
                String renamed = "/scratch/renamed-" + iteration + ".mds";
                repository.renamePath(next, renamed);
                repository.deletePath(renamed, false);
            }
            return null;
        });

        ExecutorService executor = Executors.newFixedThreadPool(tasks.size());
        try {
            List<Future<Void>> futures = executor.invokeAll(tasks);
            for (Future<Void> future : futures) {
                future.get();
            }
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }

        assertEquals(stable.fileHash(), repository.manifest("/datasets/train/stable.mds").orElseThrow().fileHash());
        assertArrayEquals(stableBytes, repository.reconstruct(stable.fileHash()));
    }

    private static void assertEntry(FileMetadata metadata, String path, String name, FileType type, long size) {
        assertEquals(path, metadata.path());
        assertEquals(name, metadata.name());
        assertEquals(type, metadata.type());
        assertEquals(size, metadata.size());
    }

    private static byte[] repeated(String value, int times) {
        return repeatedString(value, times).getBytes(StandardCharsets.UTF_8);
    }

    private static String repeatedString(String value, int times) {
        return value.repeat(times);
    }

    private static List<Chunk> fixedFourByteChunks(byte[] data) {
        List<Chunk> chunks = new ArrayList<>();
        for (int offset = 0; offset < data.length; offset += 4) {
            byte[] chunk = Arrays.copyOfRange(data, offset, Math.min(data.length, offset + 4));
            chunks.add(new Chunk(Hashing.chunk(chunk), offset, chunk));
        }
        return chunks;
    }

    private static final class CountingCodec implements CompressionCodec {
        private static final byte ID = 42;

        private final AtomicInteger decompressions = new AtomicInteger();

        @Override
        public byte id() {
            return ID;
        }

        @Override
        public String name() {
            return "counting";
        }

        @Override
        public byte[] compress(byte[] input) {
            return Arrays.copyOf(input, input.length);
        }

        @Override
        public byte[] decompress(byte[] input, int expectedSize) {
            decompressions.incrementAndGet();
            if (input.length != expectedSize) {
                throw new IllegalArgumentException("counting payload size does not match expected size");
            }
            return Arrays.copyOf(input, input.length);
        }

        int decompressions() {
            return decompressions.get();
        }
    }
}
