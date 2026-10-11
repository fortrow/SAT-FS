package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.namespace.NamespacePath;
import com.fortrow.cxnfs.protocol.TransferPayloadFormat;

import java.util.UUID;

public record AdmittedReadTransfer(
        UUID transferId,
        UUID sessionId,
        String principal,
        String path,
        TransferPayloadFormat payloadFormat,
        int acceptedInitialCords,
        int acceptedMaxCords,
        long expectedSizeLimitBytes
) {
    public AdmittedReadTransfer {
        if (transferId == null) {
            throw new IllegalArgumentException("transferId is required");
        }
        if (sessionId == null) {
            throw new IllegalArgumentException("sessionId is required");
        }
        if (principal == null || principal.isBlank()) {
            throw new IllegalArgumentException("principal is required");
        }
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("path is required");
        }
        if (payloadFormat == null) {
            throw new IllegalArgumentException("payloadFormat is required");
        }
        if (payloadFormat == TransferPayloadFormat.SAFETENSORS) {
            throw new IllegalArgumentException("read transfers do not use safetensors payloads");
        }
        if (acceptedInitialCords <= 0) {
            throw new IllegalArgumentException("acceptedInitialCords must be positive");
        }
        if (acceptedMaxCords <= 0 || acceptedMaxCords > CordPoolConfig.ABSOLUTE_MAX_CORDS) {
            throw new IllegalArgumentException("acceptedMaxCords must be between 1 and "
                    + CordPoolConfig.ABSOLUTE_MAX_CORDS);
        }
        if (acceptedInitialCords > acceptedMaxCords) {
            throw new IllegalArgumentException("acceptedInitialCords must be <= acceptedMaxCords");
        }
        if (expectedSizeLimitBytes < 0) {
            throw new IllegalArgumentException("expectedSizeLimitBytes must be non-negative");
        }
        path = normalizePath(path);
    }

    private static String normalizePath(String path) {
        return NamespacePath.normalize(path);
    }
}
