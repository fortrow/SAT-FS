package com.fortrow.cxnfs.transport;

import java.util.UUID;

public record BulkWriteAckSubscription(UUID transferId) {
    public BulkWriteAckSubscription {
        if (transferId == null) {
            throw new IllegalArgumentException("transferId is required");
        }
    }
}
