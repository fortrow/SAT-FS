package com.fortrow.cxnfs.transport;

import java.util.UUID;

public record BulkReadDeliveryResult(
        UUID transferId,
        int entrySequence,
        int activeCords,
        int frameCount,
        long payloadBytes
) {
    public BulkReadDeliveryResult {
        if (transferId == null) {
            throw new IllegalArgumentException("transferId is required");
        }
        if (entrySequence < 0) {
            throw new IllegalArgumentException("entrySequence must be non-negative");
        }
        if (activeCords < 0 || activeCords > CordPoolConfig.ABSOLUTE_MAX_CORDS) {
            throw new IllegalArgumentException("activeCords must be between 0 and " + CordPoolConfig.ABSOLUTE_MAX_CORDS);
        }
        if (frameCount < 0) {
            throw new IllegalArgumentException("frameCount must be non-negative");
        }
        if (payloadBytes < 0) {
            throw new IllegalArgumentException("payloadBytes must be non-negative");
        }
    }
}
