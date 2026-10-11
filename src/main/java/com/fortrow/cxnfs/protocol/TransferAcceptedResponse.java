package com.fortrow.cxnfs.protocol;

import com.fortrow.cxnfs.transport.CordPoolConfig;

import java.util.UUID;

public record TransferAcceptedResponse(
        UUID transferId,
        TransferDirection direction,
        int acceptedInitialCords,
        int acceptedMaxCords,
        long flowControlWindowBytes,
        boolean writePayloadAdmissionGranted
) implements ControlPayload {
    public TransferAcceptedResponse {
        if (transferId == null) {
            throw new IllegalArgumentException("transferId is required");
        }
        if (direction == null) {
            throw new IllegalArgumentException("direction is required");
        }
        if (acceptedInitialCords <= 0) {
            throw new IllegalArgumentException("acceptedInitialCords must be positive");
        }
        if (acceptedMaxCords <= 0 || acceptedMaxCords > CordPoolConfig.ABSOLUTE_MAX_CORDS) {
            throw new IllegalArgumentException("acceptedMaxCords must be between 1 and " + CordPoolConfig.ABSOLUTE_MAX_CORDS);
        }
        if (acceptedInitialCords > acceptedMaxCords) {
            throw new IllegalArgumentException("acceptedInitialCords must be <= acceptedMaxCords");
        }
        if (flowControlWindowBytes <= 0) {
            throw new IllegalArgumentException("flowControlWindowBytes must be positive");
        }
        if (direction == TransferDirection.READ && writePayloadAdmissionGranted) {
            throw new IllegalArgumentException("read transfers cannot admit write payloads");
        }
    }

    @Override
    public ControlOperation operation() {
        return ControlOperation.TRANSFER_ACCEPTED;
    }
}
