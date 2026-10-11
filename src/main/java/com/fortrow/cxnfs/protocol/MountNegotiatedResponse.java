package com.fortrow.cxnfs.protocol;

import com.fortrow.cxnfs.mount.MountStrategy;

public record MountNegotiatedResponse(
        MountStrategy strategy
) implements ControlPayload {
    public MountNegotiatedResponse {
        if (strategy == null) {
            throw new IllegalArgumentException("strategy is required");
        }
    }

    @Override
    public ControlOperation operation() {
        return ControlOperation.MOUNT_NEGOTIATED;
    }
}
