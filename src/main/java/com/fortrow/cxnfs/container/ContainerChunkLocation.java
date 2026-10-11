package com.fortrow.cxnfs.container;

import com.fortrow.cxnfs.hash.ContentHash;

public record ContainerChunkLocation(
        ContentHash containerHash,
        int chunkIndex
) {
    public ContainerChunkLocation {
        if (chunkIndex < 0) {
            throw new IllegalArgumentException("chunkIndex must be non-negative");
        }
    }
}
