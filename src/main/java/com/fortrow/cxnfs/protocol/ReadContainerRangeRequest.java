package com.fortrow.cxnfs.protocol;

import com.fortrow.cxnfs.hash.ContentHash;

public record ReadContainerRangeRequest(
        ContentHash containerHash,
        int firstChunkIndex,
        int chunkCount
) implements ControlPayload {
    public ReadContainerRangeRequest {
        if (containerHash == null) {
            throw new IllegalArgumentException("containerHash is required");
        }
        if (firstChunkIndex < 0) {
            throw new IllegalArgumentException("firstChunkIndex must be non-negative");
        }
        if (chunkCount <= 0) {
            throw new IllegalArgumentException("chunkCount must be positive");
        }
    }

    @Override
    public ControlOperation operation() {
        return ControlOperation.READ_CONTAINER_RANGE;
    }
}
