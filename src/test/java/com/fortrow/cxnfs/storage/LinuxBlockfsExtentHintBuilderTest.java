package com.fortrow.cxnfs.storage;

import com.fortrow.cxnfs.hash.ContentHash;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class LinuxBlockfsExtentHintBuilderTest {
    @Test
    void buildsExtentHintsFromManifestAndPayloadAllocationMap() {
        LinuxBlockfsExtentHintsSnapshot hints = new LinuxBlockfsExtentHintBuilder().build(
                manifest(),
                allocations(false),
                false
        );

        assertEquals(7, hints.generation());
        assertEquals(1, hints.extents().size());
        assertEquals(2, hints.extents().getFirst().inodeNumber());
        assertEquals(0, hints.extents().getFirst().fileOffset());
        assertEquals(100, hints.extents().getFirst().payloadBlock());
        assertEquals(hash(2).hex(), hints.extents().getFirst().expectedHashHex());
    }

    @Test
    void describesWhetherAllocationMapIsComplete() {
        LinuxBlockfsExtentHintBuilder builder = new LinuxBlockfsExtentHintBuilder();
        LinuxBlockfsExtentHintsSnapshot partialHints = builder.build(manifest(), allocations(false), false);
        LinuxBlockfsExtentHintBuildResult partial = builder.describe(manifest(), allocations(false), partialHints);
        LinuxBlockfsExtentHintsSnapshot completeHints = builder.build(manifest(), allocations(true), true);
        LinuxBlockfsExtentHintBuildResult complete = builder.describe(manifest(), allocations(true), completeHints);

        assertFalse(partial.complete());
        assertTrue(complete.complete());
        assertEquals(2, complete.extentHints());
    }

    @Test
    void requireCompleteRejectsPartialAllocationMap() {
        assertThrows(
                IllegalStateException.class,
                () -> new LinuxBlockfsExtentHintBuilder().build(manifest(), allocations(false), true)
        );
    }

    @Test
    void rejectsGenerationMismatch() {
        assertThrows(
                IllegalStateException.class,
                () -> new LinuxBlockfsExtentHintBuilder().build(
                        manifest(),
                        new LinuxBlockfsPayloadAllocationSnapshot(
                                8,
                                List.of(new LinuxBlockfsPayloadAllocation(2, 0, 100, 0, 4096))
                        ),
                        false
                )
        );
    }

    @Test
    void rejectsAllocationThatDoesNotMatchManifestChunk() {
        assertThrows(
                IllegalStateException.class,
                () -> new LinuxBlockfsExtentHintBuilder().build(
                        manifest(),
                        new LinuxBlockfsPayloadAllocationSnapshot(
                                7,
                                List.of(new LinuxBlockfsPayloadAllocation(2, 1024, 100, 0, 4096))
                        ),
                        false
                )
        );
    }

    private static LinuxBlockfsManifestIndexSnapshot manifest() {
        return new LinuxBlockfsManifestIndexSnapshot(
                7,
                List.of(new LinuxBlockfsManifestEntry(
                        2,
                        "datasets/train/sample.bin",
                        6144,
                        hash(1),
                        List.of(
                                new LinuxBlockfsManifestChunk(0, 4096, hash(2)),
                                new LinuxBlockfsManifestChunk(4096, 2048, hash(3))
                        )
                ))
        );
    }

    private static LinuxBlockfsPayloadAllocationSnapshot allocations(boolean complete) {
        return new LinuxBlockfsPayloadAllocationSnapshot(
                7,
                complete
                        ? List.of(
                                new LinuxBlockfsPayloadAllocation(2, 0, 100, 0, 4096),
                                new LinuxBlockfsPayloadAllocation(2, 4096, 101, 0, 2048)
                        )
                        : List.of(new LinuxBlockfsPayloadAllocation(2, 0, 100, 0, 4096))
        );
    }

    private static ContentHash hash(int seed) {
        byte[] bytes = new byte[ContentHash.BYTE_LENGTH];
        bytes[0] = (byte) seed;
        return ContentHash.of(bytes);
    }
}
