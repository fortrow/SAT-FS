package com.fortrow.cxnfs.protocol;

import java.util.UUID;

public record TransferMetadataAcceptedResponse(
        UUID transferId,
        int entrySequence,
        int metadataSequence,
        int chunkHashesAccepted,
        int stagedPayloadsVerified,
        boolean finalMetadata
) implements ControlPayload {
    public TransferMetadataAcceptedResponse {
        if (transferId == null) {
            throw new IllegalArgumentException("transferId is required");
        }
        if (entrySequence < 0) {
            throw new IllegalArgumentException("entrySequence must be non-negative");
        }
        if (metadataSequence < 0) {
            throw new IllegalArgumentException("metadataSequence must be non-negative");
        }
        if (chunkHashesAccepted < 0) {
            throw new IllegalArgumentException("chunkHashesAccepted must be non-negative");
        }
        if (stagedPayloadsVerified < 0) {
            throw new IllegalArgumentException("stagedPayloadsVerified must be non-negative");
        }
    }

    @Override
    public ControlOperation operation() {
        return ControlOperation.TRANSFER_METADATA_ACCEPTED;
    }
}
