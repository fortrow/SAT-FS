package com.fortrow.cxnfs.protocol;

import com.fortrow.cxnfs.io.ReadPlan;

public record ReadFileRangeResponse(
        String path,
        ReadPlan plan
) implements ControlPayload {
    public ReadFileRangeResponse {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("path is required");
        }
        if (plan == null) {
            throw new IllegalArgumentException("plan is required");
        }
    }

    @Override
    public ControlOperation operation() {
        return ControlOperation.READ_FILE_RANGE_RESULT;
    }
}
