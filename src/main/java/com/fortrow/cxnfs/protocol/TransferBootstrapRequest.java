package com.fortrow.cxnfs.protocol;

import com.fortrow.cxnfs.transport.CordPoolConfig;

import java.util.UUID;

public record TransferBootstrapRequest(
        UUID transferId,
        TransferDirection direction,
        String path,
        TransferPayloadFormat payloadFormat,
        int initialCords,
        int maxCords,
        int provisionalContainerSizeBytes,
        long expectedSizeLimitBytes
) implements ControlPayload {
    public TransferBootstrapRequest {
        if (transferId == null) {
            throw new IllegalArgumentException("transferId is required");
        }
        if (direction == null) {
            throw new IllegalArgumentException("direction is required");
        }
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("path is required");
        }
        if (payloadFormat == null) {
            throw new IllegalArgumentException("payloadFormat is required");
        }
        if (initialCords <= 0) {
            throw new IllegalArgumentException("initialCords must be positive");
        }
        if (maxCords <= 0 || maxCords > CordPoolConfig.ABSOLUTE_MAX_CORDS) {
            throw new IllegalArgumentException("maxCords must be between 1 and " + CordPoolConfig.ABSOLUTE_MAX_CORDS);
        }
        if (initialCords > maxCords) {
            throw new IllegalArgumentException("initialCords must be <= maxCords");
        }
        if (provisionalContainerSizeBytes <= 0) {
            throw new IllegalArgumentException("provisionalContainerSizeBytes must be positive");
        }
        if (expectedSizeLimitBytes < 0) {
            throw new IllegalArgumentException("expectedSizeLimitBytes must be non-negative");
        }
        if (direction == TransferDirection.READ && payloadFormat == TransferPayloadFormat.SAFETENSORS) {
            throw new IllegalArgumentException("read transfers do not use safetensors payloads");
        }
        if (direction == TransferDirection.WRITE && payloadFormat == TransferPayloadFormat.MDS_SHARDS) {
            throw new IllegalArgumentException("write transfers do not use mds-shards payloads");
        }
    }

    @Override
    public ControlOperation operation() {
        return ControlOperation.TRANSFER_BOOTSTRAP;
    }
}
