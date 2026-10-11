package com.fortrow.cxnfs.storage;

import com.fortrow.cxnfs.hash.ContentHash;

public record LinuxBlockfsManifestChunk(
        long fileOffset,
        int length,
        ContentHash chunkHash
) {
    public LinuxBlockfsManifestChunk {
        if (fileOffset < 0) {
            throw new IllegalArgumentException("fileOffset must be non-negative");
        }
        if (length <= 0) {
            throw new IllegalArgumentException("length must be positive");
        }
        if (chunkHash == null) {
            throw new IllegalArgumentException("chunkHash is required");
        }
    }
}
