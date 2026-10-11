package com.fortrow.cxnfs.protocol;

import com.fortrow.cxnfs.hash.ContentHash;

public record ReconstructionPlanRequest(
        ContentHash fileHash
) implements ControlPayload {
    public ReconstructionPlanRequest {
        if (fileHash == null) {
            throw new IllegalArgumentException("fileHash is required");
        }
    }

    @Override
    public ControlOperation operation() {
        return ControlOperation.GET_RECONSTRUCTION_PLAN;
    }
}
