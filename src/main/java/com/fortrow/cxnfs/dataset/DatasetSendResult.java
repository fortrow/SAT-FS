package com.fortrow.cxnfs.dataset;

import com.fortrow.cxnfs.hash.ContentHash;

import java.nio.file.Path;

public record DatasetSendResult(
        Path outputDirectory,
        String remoteRoot,
        int fileCount,
        int tensorFileCount,
        long bytesSent,
        ContentHash contentHash
) {
    public DatasetSendResult {
        if (outputDirectory == null) {
            throw new IllegalArgumentException("outputDirectory is required");
        }
        if (remoteRoot == null || remoteRoot.isBlank()) {
            throw new IllegalArgumentException("remoteRoot is required");
        }
        if (fileCount < 0) {
            throw new IllegalArgumentException("fileCount must be non-negative");
        }
        if (tensorFileCount < 0) {
            throw new IllegalArgumentException("tensorFileCount must be non-negative");
        }
        if (bytesSent < 0) {
            throw new IllegalArgumentException("bytesSent must be non-negative");
        }
        if (contentHash == null) {
            throw new IllegalArgumentException("contentHash is required");
        }
    }
}
