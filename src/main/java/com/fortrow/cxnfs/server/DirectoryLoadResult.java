package com.fortrow.cxnfs.server;

public record DirectoryLoadResult(
        long filesLoaded,
        long bytesLoaded
) {
    public DirectoryLoadResult {
        if (filesLoaded < 0) {
            throw new IllegalArgumentException("filesLoaded must be non-negative");
        }
        if (bytesLoaded < 0) {
            throw new IllegalArgumentException("bytesLoaded must be non-negative");
        }
    }
}
