package com.fortrow.cxnfs.transport;

public record TransferBackpressureSignals(
        int activeCords,
        int diskQueueDepth,
        long bytesInFlight,
        long observedThroughputBytesPerSecond,
        long p95DiskLatencyMillis
) {
    public TransferBackpressureSignals {
        if (activeCords <= 0) {
            throw new IllegalArgumentException("activeCords must be positive");
        }
        if (diskQueueDepth < 0) {
            throw new IllegalArgumentException("diskQueueDepth must be non-negative");
        }
        if (bytesInFlight < 0) {
            throw new IllegalArgumentException("bytesInFlight must be non-negative");
        }
        if (observedThroughputBytesPerSecond < 0) {
            throw new IllegalArgumentException("observedThroughputBytesPerSecond must be non-negative");
        }
        if (p95DiskLatencyMillis < 0) {
            throw new IllegalArgumentException("p95DiskLatencyMillis must be non-negative");
        }
    }
}
