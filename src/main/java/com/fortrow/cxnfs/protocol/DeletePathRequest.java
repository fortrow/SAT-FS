package com.fortrow.cxnfs.protocol;

public record DeletePathRequest(
        String path,
        boolean recursive
) implements ControlPayload {
    public DeletePathRequest {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("path is required");
        }
    }

    @Override
    public ControlOperation operation() {
        return ControlOperation.DELETE_PATH;
    }
}
