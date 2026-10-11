package com.fortrow.cxnfs.protocol;

public record ReadFileRangeRequest(
        String path,
        long offset,
        long length
) implements ControlPayload {
    public ReadFileRangeRequest {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("path is required");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be non-negative");
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative");
        }
    }

    @Override
    public ControlOperation operation() {
        return ControlOperation.READ_FILE_RANGE;
    }
}
