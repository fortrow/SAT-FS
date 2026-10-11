package com.fortrow.cxnfs.protocol;

import com.fortrow.cxnfs.hash.ContentHash;

import java.util.List;
import java.util.UUID;

public record TransferMetadataUpdate(
        UUID transferId,
        int entrySequence,
        int metadataSequence,
        boolean finalMetadata,
        long bytesCovered,
        List<ContentHash> chunkHashes
) implements ControlPayload {
    public TransferMetadataUpdate {
        if (transferId == null) {
            throw new IllegalArgumentException("transferId is required");
        }
        if (entrySequence < 0) {
            throw new IllegalArgumentException("entrySequence must be non-negative");
        }
        if (metadataSequence < 0) {
            throw new IllegalArgumentException("metadataSequence must be non-negative");
        }
        if (bytesCovered < 0) {
            throw new IllegalArgumentException("bytesCovered must be non-negative");
        }
        chunkHashes = chunkHashes == null ? List.of() : List.copyOf(chunkHashes);
        if (finalMetadata && chunkHashes.isEmpty() && bytesCovered > 0) {
            throw new IllegalArgumentException("final metadata for non-empty data must include chunk hashes");
        }
    }

    @Override
    public ControlOperation operation() {
        return ControlOperation.TRANSFER_METADATA;
    }
}
