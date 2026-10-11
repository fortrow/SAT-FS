package com.fortrow.cxnfs.dataset;

public record DatasetSourceFile(
        String relativePath,
        long sizeBytes
) {
    public DatasetSourceFile {
        if (relativePath == null || relativePath.isBlank()) {
            throw new IllegalArgumentException("relativePath is required");
        }
        if (sizeBytes < 0) {
            throw new IllegalArgumentException("sizeBytes must be non-negative");
        }
    }
}
