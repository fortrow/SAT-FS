package com.fortrow.cxnfs.chunk;

public record ChunkSettings(
        int minimumSize,
        int targetSize,
        int maximumSize,
        long boundaryMask
) {
    public static ChunkSettings xetLikeDefaults() {
        return new ChunkSettings(8 * 1024, 64 * 1024, 128 * 1024, 0xffff_0000_0000_0000L);
    }

    public ChunkSettings {
        if (minimumSize <= 0) {
            throw new IllegalArgumentException("minimumSize must be positive");
        }
        if (targetSize < minimumSize) {
            throw new IllegalArgumentException("targetSize must be >= minimumSize");
        }
        if (maximumSize < targetSize) {
            throw new IllegalArgumentException("maximumSize must be >= targetSize");
        }
        if (boundaryMask == 0) {
            throw new IllegalArgumentException("boundaryMask must be non-zero");
        }
    }
}
