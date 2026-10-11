package com.fortrow.cxnfs.transport;

import java.util.UUID;

public record AdmittedWriteTransfer(
        UUID transferId,
        UUID sessionId,
        String principal,
        String rootPath,
        int maxCords,
        long expectedSizeLimitBytes
) {
    public AdmittedWriteTransfer {
        if (transferId == null) {
            throw new IllegalArgumentException("transferId is required");
        }
        if (sessionId == null) {
            throw new IllegalArgumentException("sessionId is required");
        }
        if (principal == null || principal.isBlank()) {
            throw new IllegalArgumentException("principal is required");
        }
        if (rootPath == null || rootPath.isBlank()) {
            throw new IllegalArgumentException("rootPath is required");
        }
        if (maxCords <= 0 || maxCords > CordPoolConfig.ABSOLUTE_MAX_CORDS) {
            throw new IllegalArgumentException("maxCords must be between 1 and " + CordPoolConfig.ABSOLUTE_MAX_CORDS);
        }
        if (expectedSizeLimitBytes < 0) {
            throw new IllegalArgumentException("expectedSizeLimitBytes must be non-negative");
        }
    }
}
