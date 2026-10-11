package com.fortrow.cxnfs.protocol;

import com.fortrow.cxnfs.hash.ContentHash;

import java.util.UUID;

public record TransferEntryCommitRequest(
        UUID transferId,
        int entrySequence,
        String path,
        ContentHash contentHash,
        long bytesWritten,
        TransferChecksum checksum
) implements ControlPayload {
    public TransferEntryCommitRequest(
            UUID transferId,
            int entrySequence,
            String path,
            ContentHash contentHash,
            long bytesWritten
    ) {
        this(transferId, entrySequence, path, contentHash, bytesWritten, TransferChecksum.none());
    }

    public TransferEntryCommitRequest {
        if (transferId == null) {
            throw new IllegalArgumentException("transferId is required");
        }
        if (entrySequence < 0) {
            throw new IllegalArgumentException("entrySequence must be non-negative");
        }
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("path is required");
        }
        if (contentHash == null) {
            throw new IllegalArgumentException("contentHash is required");
        }
        if (bytesWritten < 0) {
            throw new IllegalArgumentException("bytesWritten must be non-negative");
        }
        checksum = checksum == null ? TransferChecksum.none() : checksum;
    }

    @Override
    public ControlOperation operation() {
        return ControlOperation.TRANSFER_ENTRY_COMMIT;
    }
}
