package com.fortrow.cxnfs.protocol;

import java.util.UUID;

public record TransferEntryAcceptedResponse(
        UUID transferId,
        int entrySequence,
        String path
) implements ControlPayload {
    public TransferEntryAcceptedResponse {
        if (transferId == null) {
            throw new IllegalArgumentException("transferId is required");
        }
        if (entrySequence < 0) {
            throw new IllegalArgumentException("entrySequence must be non-negative");
        }
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("path is required");
        }
    }

    @Override
    public ControlOperation operation() {
        return ControlOperation.TRANSFER_ENTRY_ACCEPTED;
    }
}
