package com.fortrow.cxnfs.protocol;

public record DeletePathResponse(
        String path,
        long deletedEntries
) implements ControlPayload {
    public DeletePathResponse {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("path is required");
        }
        if (deletedEntries < 0) {
            throw new IllegalArgumentException("deletedEntries must be non-negative");
        }
    }

    @Override
    public ControlOperation operation() {
        return ControlOperation.DELETE_PATH_RESULT;
    }
}
