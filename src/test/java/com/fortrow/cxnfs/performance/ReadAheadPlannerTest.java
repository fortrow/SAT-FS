package com.fortrow.cxnfs.performance;

import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.io.ByteRange;
import com.fortrow.cxnfs.manifest.ChunkRef;
import com.fortrow.cxnfs.manifest.FileManifest;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class ReadAheadPlannerTest {
    @Test
    void prefetchExpandsForwardWithoutChangingRequestedRead() {
        FileManifest manifest = manifest(1024 * 1024, 64 * 1024);
        ReadAheadPolicy policy = new ReadAheadPolicy(256 * 1024, 512 * 1024);

        ReadAheadPlan plan = new ReadAheadPlanner().plan(manifest, 128 * 1024, 64 * 1024, policy);

        assertEquals(new ByteRange(128 * 1024, 64 * 1024), plan.requested().requestedRange());
        assertEquals(new ByteRange(128 * 1024, 320 * 1024), plan.prefetch().requestedRange());
        assertEquals(1, plan.requested().chunks().size());
        assertEquals(5, plan.prefetch().chunks().size());
    }

    @Test
    void prefetchIsClampedToFileSize() {
        FileManifest manifest = manifest(512 * 1024, 64 * 1024);
        ReadAheadPolicy policy = new ReadAheadPolicy(256 * 1024, 512 * 1024);

        ReadAheadPlan plan = new ReadAheadPlanner().plan(manifest, 448 * 1024, 64 * 1024, policy);

        assertEquals(new ByteRange(448 * 1024, 64 * 1024), plan.requested().requestedRange());
        assertEquals(new ByteRange(448 * 1024, 64 * 1024), plan.prefetch().requestedRange());
    }

    @Test
    void prefetchRespectsMaxPrefetchBytes() {
        FileManifest manifest = manifest(10 * 1024 * 1024, 64 * 1024);
        ReadAheadPolicy policy = new ReadAheadPolicy(2 * 1024 * 1024, 512 * 1024);

        ReadAheadPlan plan = new ReadAheadPlanner().plan(manifest, 0, 64 * 1024, policy);

        assertEquals(new ByteRange(0, 512 * 1024), plan.prefetch().requestedRange());
    }

    private static FileManifest manifest(int size, int chunkSize) {
        int chunks = size / chunkSize;
        return new FileManifest(
                "/dataset.bin",
                hash(1),
                size,
                java.util.stream.IntStream.range(0, chunks)
                        .mapToObj(index -> new ChunkRef(hash(index + 2), chunkSize))
                        .toList()
        );
    }

    private static ContentHash hash(int seed) {
        byte[] bytes = new byte[ContentHash.BYTE_LENGTH];
        bytes[0] = (byte) seed;
        return ContentHash.of(bytes);
    }
}
