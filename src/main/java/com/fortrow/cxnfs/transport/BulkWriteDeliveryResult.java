package com.fortrow.cxnfs.transport;

public record BulkWriteDeliveryResult(
        BulkDataFrameKey key,
        int payloadLength,
        boolean metadataVerified,
        StagedPayloadStorage storage
) {
    public BulkWriteDeliveryResult {
        if (key == null) {
            throw new IllegalArgumentException("key is required");
        }
        if (payloadLength <= 0) {
            throw new IllegalArgumentException("payloadLength must be positive");
        }
        if (storage == null) {
            throw new IllegalArgumentException("storage is required");
        }
    }
}
