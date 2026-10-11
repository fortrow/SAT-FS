package com.fortrow.cxnfs.server;

import com.fortrow.cxnfs.chunk.GearHashChunker;
import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.io.ReadPlan;
import com.fortrow.cxnfs.io.StagedWrite;
import com.fortrow.cxnfs.manifest.ChunkRef;
import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.metadata.DirectoryListing;
import com.fortrow.cxnfs.metadata.FileMetadata;
import com.fortrow.cxnfs.reconstruction.ReconstructionPlan;
import com.fortrow.cxnfs.store.CxnfsRepository;
import com.fortrow.cxnfs.store.CxnfsStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

final class DirectoryRepositoryLoaderTest {
    @TempDir
    Path tempDir;

    @Test
    void loadsDirectoryFilesThroughStreamingStorePath() throws Exception {
        Path root = tempDir.resolve("export");
        Files.createDirectories(root.resolve("nested"));
        Files.writeString(root.resolve("nested/a.mds"), "alpha", StandardCharsets.UTF_8);
        Files.writeString(root.resolve("nested/b.mds"), "beta", StandardCharsets.UTF_8);
        StreamingOnlyStore store = new StreamingOnlyStore();

        DirectoryLoadResult result = new DirectoryRepositoryLoader().load(root, store);

        assertEquals(2, result.filesLoaded());
        assertEquals(9, result.bytesLoaded());
        assertEquals(Set.of("/nested/a.mds", "/nested/b.mds"), store.streamedPaths);
        assertEquals(Set.of("/nested/a.mds", "/nested/b.mds"), store.retainedPaths);
        assertEquals(9, store.streamedBytes);
    }

    @Test
    void loadsMultipleJailedRootsThroughRemotePrefixes() throws Exception {
        Path datasets = tempDir.resolve("datasets");
        Path outputs = tempDir.resolve("outputs");
        Files.createDirectories(datasets.resolve("train"));
        Files.createDirectories(outputs.resolve("run-1"));
        Files.writeString(datasets.resolve("train/a.mds"), "alpha", StandardCharsets.UTF_8);
        Files.writeString(outputs.resolve("run-1/model.bin"), "weights", StandardCharsets.UTF_8);
        StreamingOnlyStore store = new StreamingOnlyStore();

        DirectoryLoadResult result = new DirectoryRepositoryLoader().load(
                List.of(
                        new ServedRoot("/datasets", datasets),
                        new ServedRoot("/outputs", outputs)
                ),
                store
        );

        assertEquals(2, result.filesLoaded());
        assertEquals(12, result.bytesLoaded());
        assertEquals(Set.of("/datasets/train/a.mds", "/outputs/run-1/model.bin"), store.streamedPaths);
        assertEquals(Set.of("/datasets/train/a.mds", "/outputs/run-1/model.bin"), store.retainedPaths);
    }

    @Test
    void aggregateRootListsAllServedRootPrefixes() throws Exception {
        Path datasets = tempDir.resolve("datasets-list");
        Path outputs = tempDir.resolve("outputs-list");
        Files.createDirectories(datasets.resolve("train"));
        Files.createDirectories(outputs.resolve("run-1"));
        Files.writeString(datasets.resolve("train/a.mds"), "alpha", StandardCharsets.UTF_8);
        Files.writeString(outputs.resolve("run-1/model.bin"), "weights", StandardCharsets.UTF_8);
        CxnfsRepository repository = new CxnfsRepository(GearHashChunker.defaults());

        new DirectoryRepositoryLoader().load(
                List.of(
                        new ServedRoot("/datasets", datasets),
                        new ServedRoot("/outputs", outputs)
                ),
                repository
        );

        Set<String> rootPaths = repository.list("/")
                .entries()
                .stream()
                .map(FileMetadata::path)
                .collect(java.util.stream.Collectors.toSet());
        assertEquals(Set.of("/datasets", "/outputs"), rootPaths);
    }

    @Test
    void bareRootUsesActualServerPathAsRemotePrefix() throws Exception {
        Path datasets = tempDir.resolve("srv/datasets");
        Files.createDirectories(datasets);
        Files.writeString(datasets.resolve("a.mds"), "alpha", StandardCharsets.UTF_8);
        StreamingOnlyStore store = new StreamingOnlyStore();

        DirectoryLoadResult result = new DirectoryRepositoryLoader().load(
                List.of(new ServedRoot(datasets.toAbsolutePath().normalize().toString(), datasets)),
                store
        );

        String expected = datasets.toAbsolutePath().normalize().toString().replace('\\', '/') + "/a.mds";
        assertEquals(1, result.filesLoaded());
        assertEquals(Set.of(expected), store.streamedPaths);
    }

    @Test
    void rejectsOverlappingRemotePrefixesAndLocalJails() throws Exception {
        Path root = tempDir.resolve("root");
        Files.createDirectories(root.resolve("child"));
        StreamingOnlyStore store = new StreamingOnlyStore();

        assertThrows(IllegalArgumentException.class, () -> new DirectoryRepositoryLoader().load(
                List.of(new ServedRoot("/datasets", root), new ServedRoot("/datasets/train", root.resolve("child"))),
                store
        ));
        assertThrows(IllegalArgumentException.class, () -> new DirectoryRepositoryLoader().load(
                List.of(new ServedRoot("/datasets", root), new ServedRoot("/outputs", root.resolve("child"))),
                store
        ));
    }

    private static final class StreamingOnlyStore implements CxnfsStore {
        private final Set<String> streamedPaths = new LinkedHashSet<>();
        private final Set<String> retainedPaths = new LinkedHashSet<>();
        private long streamedBytes;

        @Override
        public FileManifest put(String path, byte[] data) {
            fail("directory loading should stream files instead of materializing them");
            return null;
        }

        @Override
        public FileManifest putStream(String path, InputStream data) throws IOException {
            streamedPaths.add(path);
            streamedBytes += data.transferTo(OutputStream.nullOutputStream());
            return null;
        }

        @Override
        public void pruneExcept(Set<String> retainedPaths) {
            this.retainedPaths.addAll(retainedPaths);
        }

        @Override
        public FileManifest commit(StagedWrite stagedWrite) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putVerifiedChunk(ContentHash chunkHash, long sourceOffset, byte[] data) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileManifest publishManifest(FileManifest manifest) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DirectoryListing list(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<FileMetadata> stat(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<FileManifest> manifest(ContentHash fileHash) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<FileManifest> manifest(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<ContentHash> missingChunks(List<ChunkRef> chunks) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ReconstructionPlan plan(ContentHash fileHash) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ReadPlan readPlan(ContentHash fileHash, long offset, long length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] readRange(ContentHash fileHash, long offset, long length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] reconstruct(ContentHash fileHash) {
            throw new UnsupportedOperationException();
        }
    }
}
