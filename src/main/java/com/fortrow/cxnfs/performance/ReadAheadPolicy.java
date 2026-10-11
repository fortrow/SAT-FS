package com.fortrow.cxnfs.performance;

public record ReadAheadPolicy(
        long forwardWindowBytes,
        long maxPrefetchBytes
) {
    public static ReadAheadPolicy linuxAiTrainingDataset() {
        return new ReadAheadPolicy(32L * 1024 * 1024, 512L * 1024 * 1024);
    }

    public ReadAheadPolicy {
        if (forwardWindowBytes < 0) {
            throw new IllegalArgumentException("forwardWindowBytes must be non-negative");
        }
        if (maxPrefetchBytes < 0) {
            throw new IllegalArgumentException("maxPrefetchBytes must be non-negative");
        }
    }
}
