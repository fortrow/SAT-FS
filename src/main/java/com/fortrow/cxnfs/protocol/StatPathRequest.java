package com.fortrow.cxnfs.protocol;

public record StatPathRequest(
        String path
) implements ControlPayload {
    public StatPathRequest {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("path is required");
        }
    }

    @Override
    public ControlOperation operation() {
        return ControlOperation.STAT_PATH;
    }
}
