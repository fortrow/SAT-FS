package com.fortrow.cxnfs.protocol;

import com.fortrow.cxnfs.reconstruction.ReconstructionPlan;

public record ReconstructionPlanResponse(
        ReconstructionPlan plan
) implements ControlPayload {
    public ReconstructionPlanResponse {
        if (plan == null) {
            throw new IllegalArgumentException("plan is required");
        }
    }

    @Override
    public ControlOperation operation() {
        return ControlOperation.RECONSTRUCTION_PLAN_RESULT;
    }
}
