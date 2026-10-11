package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.namespace.NamespacePath;

import java.util.UUID;

public record BulkReadRequest(
        UUID sessionId,
        UUID transferId,
        String path,
        long offset,
        long length,
        int activeCords
) {
    public BulkReadRequest {
        if (sessionId == null) {
            throw new IllegalArgumentException("sessionId is required");
        }
        if (transferId == null) {
            throw new IllegalArgumentException("transferId is required");
        }
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("path is required");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be non-negative");
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative");
        }
        if (activeCords <= 0 || activeCords > CordPoolConfig.ABSOLUTE_MAX_CORDS) {
            throw new IllegalArgumentException("activeCords must be between 1 and " + CordPoolConfig.ABSOLUTE_MAX_CORDS);
        }
        path = normalizePath(path);
    }

    private static String normalizePath(String path) {
        return NamespacePath.normalize(path);
    }
}
