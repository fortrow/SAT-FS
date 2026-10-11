package com.fortrow.cxnfs.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class LinuxBlockfsExtentHintStoreTest {
    @TempDir
    Path tempDir;

    @Test
    void writesAndLoadsExtentHintsFromMetadataRoot() throws Exception {
        LinuxBlockfsExtentHintStore store = new LinuxBlockfsExtentHintStore();
        LinuxBlockfsExtentHintsSnapshot snapshot = new LinuxBlockfsExtentHintsSnapshot(
                12,
                List.of(
                        new LinuxBlockfsExtent(2, 0, 1, 0, 4096, hash(1)),
                        new LinuxBlockfsExtent(2, 4096, 2, 128, 2048, hash(2))
                )
        );

        Path path = store.write(tempDir, snapshot);
        LinuxBlockfsExtentHintsSnapshot loaded = store.load(tempDir);

        assertEquals(tempDir.resolve("extent-hints/current.cxn"), path);
        assertEquals(12, loaded.generation());
        assertEquals(snapshot.extents().size(), loaded.extents().size());
        assertEquals(snapshot.extents().getFirst().expectedHashHex(), loaded.extents().getFirst().expectedHashHex());
        assertEquals(2048, loaded.extents().get(1).payloadLength());
    }

    @Test
    void missingExtentHintsAreOptionalForNamespaceSync() throws Exception {
        LinuxBlockfsExtentHintStore store = new LinuxBlockfsExtentHintStore();

        assertFalse(store.loadIfExists(tempDir).isPresent());
    }

    @Test
    void rejectsUnsupportedVersion() throws Exception {
        Path path = tempDir.resolve("extent-hints/current.cxn");
        Files.createDirectories(path.getParent());
        Files.writeString(path, """
                version=2
                generation=1
                extent.count=0
                """);

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> new LinuxBlockfsExtentHintStore().load(tempDir)
        );

        assertEquals("unsupported extent hint snapshot version: 2", error.getMessage());
    }

    @Test
    void rejectsOverlappingExtentsForSameInode() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new LinuxBlockfsExtentHintsSnapshot(
                        1,
                        List.of(
                                new LinuxBlockfsExtent(2, 0, 1, 0, 4096, hash(1)),
                                new LinuxBlockfsExtent(2, 2048, 2, 0, 4096, hash(2))
                        )
                )
        );
    }

    @Test
    void acceptsOverlappingExtentsForDifferentInodes() {
        LinuxBlockfsExtentHintsSnapshot snapshot = new LinuxBlockfsExtentHintsSnapshot(
                1,
                List.of(
                        new LinuxBlockfsExtent(2, 0, 1, 0, 4096, hash(1)),
                        new LinuxBlockfsExtent(3, 2048, 2, 0, 4096, hash(2))
                )
        );

        assertEquals(2, snapshot.extents().size());
    }

    @Test
    void rejectsConflictingPhysicalExtentsAcrossInodes() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new LinuxBlockfsExtentHintsSnapshot(
                        1,
                        List.of(
                                new LinuxBlockfsExtent(2, 0, 10, 0, 4096, hash(1)),
                                new LinuxBlockfsExtent(3, 0, 10, 2048, 4096, hash(2))
                        )
                )
        );
    }

    @Test
    void acceptsSharedPhysicalExtentWhenHashMatches() {
        LinuxBlockfsExtentHintsSnapshot snapshot = new LinuxBlockfsExtentHintsSnapshot(
                1,
                List.of(
                        new LinuxBlockfsExtent(2, 0, 10, 0, 4096, hash(1)),
                        new LinuxBlockfsExtent(3, 0, 10, 0, 4096, hash(1))
                )
        );

        assertEquals(2, snapshot.extents().size());
    }

    @Test
    void rejectsSharedPhysicalExtentWhenHashDiffers() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new LinuxBlockfsExtentHintsSnapshot(
                        1,
                        List.of(
                                new LinuxBlockfsExtent(2, 0, 10, 0, 4096, hash(1)),
                                new LinuxBlockfsExtent(3, 0, 10, 0, 4096, hash(2))
                        )
                )
        );
    }

    @Test
    void rejectsPayloadOffsetOutsideBlock() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new LinuxBlockfsExtent(2, 0, 10, 4096, 1, hash(1))
        );
    }

    private static byte[] hash(int seed) {
        byte[] hash = new byte[32];
        hash[0] = (byte) seed;
        return hash;
    }
}
