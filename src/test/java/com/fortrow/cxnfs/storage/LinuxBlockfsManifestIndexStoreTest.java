package com.fortrow.cxnfs.storage;

import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.manifest.ChunkRef;
import com.fortrow.cxnfs.manifest.FileManifest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class LinuxBlockfsManifestIndexStoreTest {
    @TempDir
    Path tempDir;

    @Test
    void writesAndLoadsManifestIndexFromMetadataRoot() throws Exception {
        LinuxBlockfsManifestIndexStore store = new LinuxBlockfsManifestIndexStore();
        LinuxBlockfsManifestIndexSnapshot snapshot = new LinuxBlockfsManifestIndexSnapshot(
                8,
                List.of(entry())
        );

        Path path = store.write(tempDir, snapshot);
        LinuxBlockfsManifestIndexSnapshot loaded = store.load(tempDir);

        assertEquals(tempDir.resolve("manifest-index/current.cxn"), path);
        assertEquals(8, loaded.generation());
        assertEquals(1, loaded.entries().size());
        assertEquals("datasets/train/sample.bin", loaded.entries().getFirst().path());
        assertEquals(2, loaded.chunkCount());
        assertEquals(hash(2), loaded.entries().getFirst().chunks().getFirst().chunkHash());
    }

    @Test
    void missingManifestIndexIsOptionalWhenNoExtentHintsExist() throws Exception {
        assertFalse(new LinuxBlockfsManifestIndexStore().loadIfExists(tempDir).isPresent());
    }

    @Test
    void rejectsUnsupportedVersion() throws Exception {
        Path path = tempDir.resolve("manifest-index/current.cxn");
        Files.createDirectories(path.getParent());
        Files.writeString(path, """
                version=2
                generation=1
                file.count=0
                """);

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> new LinuxBlockfsManifestIndexStore().load(tempDir)
        );

        assertEquals("unsupported manifest index version: 2", error.getMessage());
    }

    @Test
    void createsEntryFromFileManifestWithContiguousChunkOffsets() {
        FileManifest manifest = new FileManifest(
                "/datasets/train/sample.bin",
                hash(1),
                6144,
                List.of(
                        new ChunkRef(hash(2), 4096),
                        new ChunkRef(hash(3), 2048)
                )
        );

        LinuxBlockfsManifestEntry entry = LinuxBlockfsManifestEntry.fromManifest(2, manifest);

        assertEquals("datasets/train/sample.bin", entry.path());
        assertEquals(0, entry.chunks().get(0).fileOffset());
        assertEquals(4096, entry.chunks().get(1).fileOffset());
    }

    @Test
    void rejectsNonContiguousChunkOffsets() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new LinuxBlockfsManifestEntry(
                        2,
                        "datasets/train/sample.bin",
                        4096,
                        hash(1),
                        List.of(new LinuxBlockfsManifestChunk(128, 4096, hash(2)))
                )
        );
    }

    private static LinuxBlockfsManifestEntry entry() {
        return new LinuxBlockfsManifestEntry(
                2,
                "datasets/train/sample.bin",
                6144,
                hash(1),
                List.of(
                        new LinuxBlockfsManifestChunk(0, 4096, hash(2)),
                        new LinuxBlockfsManifestChunk(4096, 2048, hash(3))
                )
        );
    }

    private static ContentHash hash(int seed) {
        byte[] bytes = new byte[ContentHash.BYTE_LENGTH];
        bytes[0] = (byte) seed;
        return ContentHash.of(bytes);
    }
}
