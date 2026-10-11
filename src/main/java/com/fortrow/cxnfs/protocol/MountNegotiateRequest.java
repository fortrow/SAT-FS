package com.fortrow.cxnfs.protocol;

import com.fortrow.cxnfs.mount.MountBackend;
import com.fortrow.cxnfs.mount.MountCapability;

import java.util.Set;

public record MountNegotiateRequest(
        MountBackend backend,
        Set<MountCapability> requestedCapabilities
) implements ControlPayload {
    public MountNegotiateRequest {
        if (backend == null) {
            throw new IllegalArgumentException("backend is required");
        }
        requestedCapabilities = requestedCapabilities == null ? Set.of() : Set.copyOf(requestedCapabilities);
        if (requestedCapabilities.isEmpty()) {
            throw new IllegalArgumentException("at least one capability must be requested");
        }
    }

    @Override
    public ControlOperation operation() {
        return ControlOperation.MOUNT_NEGOTIATE;
    }
}
