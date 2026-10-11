package com.fortrow.cxnfs.reconstruction;

import com.fortrow.cxnfs.hash.ContentHash;

public record ReconstructionTerm(
        ContentHash containerHash,
        int firstChunkIndex,
        int chunkCount
) {
    public ReconstructionTerm {
        if (firstChunkIndex < 0) {
            throw new IllegalArgumentException("firstChunkIndex must be non-negative");
        }
        if (chunkCount <= 0) {
            throw new IllegalArgumentException("chunkCount must be positive");
        }
    }

    public int lastChunkIndexExclusive() {
        return firstChunkIndex + chunkCount;
    }
}
