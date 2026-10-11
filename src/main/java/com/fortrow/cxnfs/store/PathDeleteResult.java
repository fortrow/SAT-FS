package com.fortrow.cxnfs.store;

public record PathDeleteResult(
        String path,
        long deletedEntries
) {
    public PathDeleteResult {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("path is required");
        }
        if (deletedEntries < 0) {
            throw new IllegalArgumentException("deletedEntries must be non-negative");
        }
    }
}
