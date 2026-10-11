package com.fortrow.cxnfs.protocol;

import java.util.UUID;

public record TransferEntryUpdate(
        UUID transferId,
        int entrySequence,
        TransferEntryAction action,
        String path,
        String targetPath,
        long sizeHintBytes,
        TransferPayloadFormat payloadFormat
) implements ControlPayload {
    public TransferEntryUpdate {
        if (transferId == null) {
            throw new IllegalArgumentException("transferId is required");
        }
        if (entrySequence < 0) {
            throw new IllegalArgumentException("entrySequence must be non-negative");
        }
        if (action == null) {
            throw new IllegalArgumentException("action is required");
        }
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("path is required");
        }
        targetPath = targetPath == null ? "" : targetPath;
        if (action == TransferEntryAction.RENAME && targetPath.isBlank()) {
            throw new IllegalArgumentException("targetPath is required for rename");
        }
        if (sizeHintBytes < 0) {
            throw new IllegalArgumentException("sizeHintBytes must be non-negative");
        }
        if (payloadFormat == null) {
            throw new IllegalArgumentException("payloadFormat is required");
        }
    }

    @Override
    public ControlOperation operation() {
        return ControlOperation.TRANSFER_ENTRY_UPDATE;
    }
}
