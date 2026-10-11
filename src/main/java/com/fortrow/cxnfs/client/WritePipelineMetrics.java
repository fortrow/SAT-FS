package com.fortrow.cxnfs.client;

public record WritePipelineMetrics(
        long capacityWaitNanos,
        int maxActiveCords,
        int maxInFlightFrames,
        long maxInFlightBytes,
        long maxAdaptiveWindowBytes,
        long acknowledgedBytes,
        long ackNanos,
        long maxFrameAckNanos,
        int slowFrames,
        int windowDecreases
) {
    public static WritePipelineMetrics empty() {
        return new WritePipelineMetrics(0L, 0, 0, 0L, 0L, 0L, 0L, 0L, 0, 0);
    }
}
