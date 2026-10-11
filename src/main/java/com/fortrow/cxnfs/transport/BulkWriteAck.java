package com.fortrow.cxnfs.transport;

public record BulkWriteAck(
        BulkDataFrameKey key,
        int payloadLength,
        boolean metadataVerified,
        StagedPayloadStorage storage
) {
    public BulkWriteAck {
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

    public static BulkWriteAck from(BulkWriteDeliveryResult result) {
        if (result == null) {
            throw new IllegalArgumentException("result is required");
        }
        return new BulkWriteAck(
                result.key(),
                result.payloadLength(),
                result.metadataVerified(),
                result.storage()
        );
    }
}
