package com.fortrow.cxnfs.io;

import com.fortrow.cxnfs.hash.ContentHash;

import java.util.List;

public record ReadPlan(
        ContentHash fileHash,
        long fileSize,
        ByteRange requestedRange,
        List<PlannedChunkRead> chunks
) {
    public ReadPlan {
        if (fileHash == null) {
            throw new IllegalArgumentException("fileHash is required");
        }
        if (fileSize < 0) {
            throw new IllegalArgumentException("fileSize must be non-negative");
        }
        if (requestedRange == null) {
            throw new IllegalArgumentException("requestedRange is required");
        }
        chunks = chunks == null ? List.of() : List.copyOf(chunks);
    }
}
