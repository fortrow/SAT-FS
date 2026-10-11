package com.fortrow.cxnfs.protocol;

public record DirectoryListRequest(
        String path
) implements ControlPayload {
    public DirectoryListRequest {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("path is required");
        }
    }

    @Override
    public ControlOperation operation() {
        return ControlOperation.LIST_DIRECTORY;
    }
}
