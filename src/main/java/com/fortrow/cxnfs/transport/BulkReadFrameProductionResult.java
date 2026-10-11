package com.fortrow.cxnfs.transport;

public record BulkReadFrameProductionResult(int frameCount, long payloadBytes) {
    public BulkReadFrameProductionResult {
        if (frameCount < 0) {
            throw new IllegalArgumentException("frameCount must be non-negative");
        }
        if (payloadBytes < 0) {
            throw new IllegalArgumentException("payloadBytes must be non-negative");
        }
    }
}
