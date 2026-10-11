package com.fortrow.cxnfs.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class LinuxBlockfsPayloadAllocationStoreTest {
    @TempDir
    Path tempDir;

    @Test
    void writesAndLoadsPayloadAllocationsFromMetadataRoot() throws Exception {
        LinuxBlockfsPayloadAllocationStore store = new LinuxBlockfsPayloadAllocationStore();
        LinuxBlockfsPayloadAllocationSnapshot snapshot = new LinuxBlockfsPayloadAllocationSnapshot(
                9,
                List.of(
                        new LinuxBlockfsPayloadAllocation(2, 0, 1, 0, 4096),
                        new LinuxBlockfsPayloadAllocation(2, 4096, 2, 128, 2048)
                )
        );

        Path path = store.write(tempDir, snapshot);
        LinuxBlockfsPayloadAllocationSnapshot loaded = store.load(tempDir);

        assertEquals(tempDir.resolve("payload-map/current.cxn"), path);
        assertEquals(9, loaded.generation());
        assertEquals(2, loaded.allocations().size());
        assertEquals(128, loaded.allocations().get(1).payloadOffset());
    }

    @Test
    void missingPayloadAllocationMapIsOptionalForProbing() throws Exception {
        assertFalse(new LinuxBlockfsPayloadAllocationStore().loadIfExists(tempDir).isPresent());
    }

    @Test
    void rejectsUnsupportedVersion() throws Exception {
        Path path = tempDir.resolve("payload-map/current.cxn");
        Files.createDirectories(path.getParent());
        Files.writeString(path, """
                version=2
                generation=1
                allocation.count=0
                """);

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> new LinuxBlockfsPayloadAllocationStore().load(tempDir)
        );

        assertEquals("unsupported payload allocation version: 2", error.getMessage());
    }

    @Test
    void rejectsOverlappingAllocationsForSameInode() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new LinuxBlockfsPayloadAllocationSnapshot(
                        1,
                        List.of(
                                new LinuxBlockfsPayloadAllocation(2, 0, 1, 0, 4096),
                                new LinuxBlockfsPayloadAllocation(2, 2048, 2, 0, 4096)
                        )
                )
        );
    }

    @Test
    void rejectsOverlappingPhysicalPayloadAllocationsAcrossInodes() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new LinuxBlockfsPayloadAllocationSnapshot(
                        1,
                        List.of(
                                new LinuxBlockfsPayloadAllocation(2, 0, 10, 0, 4096),
                                new LinuxBlockfsPayloadAllocation(3, 0, 10, 2048, 4096)
                        )
                )
        );
    }

    @Test
    void acceptsAdjacentPhysicalPayloadAllocationsAcrossInodes() {
        LinuxBlockfsPayloadAllocationSnapshot snapshot = new LinuxBlockfsPayloadAllocationSnapshot(
                1,
                List.of(
                        new LinuxBlockfsPayloadAllocation(2, 0, 10, 0, 4096),
                        new LinuxBlockfsPayloadAllocation(3, 0, 11, 0, 4096)
                )
        );

        assertEquals(2, snapshot.allocations().size());
    }

    @Test
    void rejectsPayloadOffsetOutsideBlock() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new LinuxBlockfsPayloadAllocation(2, 0, 10, 4096, 1)
        );
    }
}
