package com.fortrow.cxnfs.transport;

import java.util.Optional;

public record BulkTransferDeliveryResult(
        BulkTransferMessageType type,
        Optional<BulkReadDeliveryResult> read,
        Optional<BulkWriteDeliveryResult> write
) {
    public static BulkTransferDeliveryResult read(BulkReadDeliveryResult result) {
        return new BulkTransferDeliveryResult(BulkTransferMessageType.READ_REQUEST, Optional.of(result), Optional.empty());
    }

    public static BulkTransferDeliveryResult write(BulkWriteDeliveryResult result) {
        return new BulkTransferDeliveryResult(BulkTransferMessageType.WRITE_FRAME, Optional.empty(), Optional.of(result));
    }

    public BulkTransferDeliveryResult {
        if (type == null) {
            throw new IllegalArgumentException("type is required");
        }
        read = read == null ? Optional.empty() : read;
        write = write == null ? Optional.empty() : write;
        if (type == BulkTransferMessageType.READ_REQUEST && read.isEmpty()) {
            throw new IllegalArgumentException("read result is required for read messages");
        }
        if (type == BulkTransferMessageType.WRITE_FRAME && write.isEmpty()) {
            throw new IllegalArgumentException("write result is required for write messages");
        }
    }
}
