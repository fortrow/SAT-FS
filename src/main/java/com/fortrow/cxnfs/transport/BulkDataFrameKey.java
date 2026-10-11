package com.fortrow.cxnfs.transport;

import java.util.UUID;

public record BulkDataFrameKey(
        UUID transferId,
        int entrySequence,
        long chunkSequence
) {
    public BulkDataFrameKey {
        if (transferId == null) {
            throw new IllegalArgumentException("transferId is required");
        }
        if (entrySequence < 0) {
            throw new IllegalArgumentException("entrySequence must be non-negative");
        }
        if (chunkSequence < 0) {
            throw new IllegalArgumentException("chunkSequence must be non-negative");
        }
    }
}
