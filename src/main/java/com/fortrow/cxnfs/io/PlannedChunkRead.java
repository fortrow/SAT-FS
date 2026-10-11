package com.fortrow.cxnfs.io;

import com.fortrow.cxnfs.hash.ContentHash;

public record PlannedChunkRead(
        int chunkIndex,
        ContentHash chunkHash,
        long fileOffset,
        int chunkSize,
        int offsetInChunk,
        int bytesToRead
) {
    public PlannedChunkRead {
        if (chunkIndex < 0) {
            throw new IllegalArgumentException("chunkIndex must be non-negative");
        }
        if (chunkHash == null) {
            throw new IllegalArgumentException("chunkHash is required");
        }
        if (fileOffset < 0) {
            throw new IllegalArgumentException("fileOffset must be non-negative");
        }
        if (chunkSize < 0) {
            throw new IllegalArgumentException("chunkSize must be non-negative");
        }
        if (offsetInChunk < 0 || offsetInChunk > chunkSize) {
            throw new IllegalArgumentException("offsetInChunk must be inside chunk");
        }
        if (bytesToRead < 0 || offsetInChunk + bytesToRead > chunkSize) {
            throw new IllegalArgumentException("bytesToRead must fit inside chunk");
        }
    }

    public long readFileOffset() {
        return fileOffset + offsetInChunk;
    }
}
