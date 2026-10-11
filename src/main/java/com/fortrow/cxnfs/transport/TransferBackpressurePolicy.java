package com.fortrow.cxnfs.transport;

public record TransferBackpressurePolicy(
        int maxDiskQueueDepth,
        long maxBytesInFlight,
        long maxP95DiskLatencyMillis,
        long minThroughputBytesPerSecond
) {
    public static TransferBackpressurePolicy defaults() {
        return new TransferBackpressurePolicy(
                128,
                256L * 1024 * 1024,
                250,
                8L * 1024 * 1024
        );
    }

    public TransferBackpressurePolicy {
        if (maxDiskQueueDepth <= 0) {
            throw new IllegalArgumentException("maxDiskQueueDepth must be positive");
        }
        if (maxBytesInFlight <= 0) {
            throw new IllegalArgumentException("maxBytesInFlight must be positive");
        }
        if (maxP95DiskLatencyMillis <= 0) {
            throw new IllegalArgumentException("maxP95DiskLatencyMillis must be positive");
        }
        if (minThroughputBytesPerSecond < 0) {
            throw new IllegalArgumentException("minThroughputBytesPerSecond must be non-negative");
        }
    }
}
