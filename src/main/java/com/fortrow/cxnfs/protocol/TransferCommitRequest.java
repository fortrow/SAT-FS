package com.fortrow.cxnfs.protocol;

import com.fortrow.cxnfs.hash.ContentHash;

import java.util.UUID;

public record TransferCommitRequest(
        UUID transferId,
        String path,
        ContentHash contentHash,
        long bytesWritten
) implements ControlPayload {
    public TransferCommitRequest {
        if (transferId == null) {
            throw new IllegalArgumentException("transferId is required");
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
    }

    @Override
    public ControlOperation operation() {
        return ControlOperation.TRANSFER_COMMIT;
    }
}
