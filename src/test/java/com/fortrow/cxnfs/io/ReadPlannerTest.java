package com.fortrow.cxnfs.io;

import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.manifest.ChunkRef;
import com.fortrow.cxnfs.manifest.FileManifest;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ReadPlannerTest {
    @Test
    void mapsByteRangeAcrossChunkBoundaries() {
        FileManifest manifest = new FileManifest(
                "/big.bin",
                hash(1),
                100,
                List.of(new ChunkRef(hash(2), 40), new ChunkRef(hash(3), 30), new ChunkRef(hash(4), 30))
        );

        ReadPlan plan = new ReadPlanner().plan(manifest, 35, 45);

        assertEquals(new ByteRange(35, 45), plan.requestedRange());
        assertEquals(3, plan.chunks().size());
        assertChunk(plan.chunks().get(0), 0, 0, 40, 35, 5);
        assertChunk(plan.chunks().get(1), 1, 40, 30, 0, 30);
        assertChunk(plan.chunks().get(2), 2, 70, 30, 0, 10);
    }

    @Test
    void clampsReadsPastEndOfFile() {
        FileManifest manifest = new FileManifest(
                "/big.bin",
                hash(1),
                50,
                List.of(new ChunkRef(hash(2), 25), new ChunkRef(hash(3), 25))
        );

        ReadPlan plan = new ReadPlanner().plan(manifest, 40, 100);

        assertEquals(new ByteRange(40, 10), plan.requestedRange());
        assertEquals(1, plan.chunks().size());
        assertChunk(plan.chunks().getFirst(), 1, 25, 25, 15, 10);
    }

    @Test
    void readStartingPastEndProducesEmptyPlan() {
        FileManifest manifest = new FileManifest("/empty-at-end.bin", hash(1), 10, List.of(new ChunkRef(hash(2), 10)));

        ReadPlan plan = new ReadPlanner().plan(manifest, 11, 10);

        assertEquals(new ByteRange(10, 0), plan.requestedRange());
        assertTrue(plan.chunks().isEmpty());
    }

    private static void assertChunk(
            PlannedChunkRead chunk,
            int chunkIndex,
            long fileOffset,
            int chunkSize,
            int offsetInChunk,
            int bytesToRead
    ) {
        assertEquals(chunkIndex, chunk.chunkIndex());
        assertEquals(fileOffset, chunk.fileOffset());
        assertEquals(chunkSize, chunk.chunkSize());
        assertEquals(offsetInChunk, chunk.offsetInChunk());
        assertEquals(bytesToRead, chunk.bytesToRead());
    }

    private static ContentHash hash(int seed) {
        byte[] bytes = new byte[ContentHash.BYTE_LENGTH];
        bytes[0] = (byte) seed;
        return ContentHash.of(bytes);
    }
}
