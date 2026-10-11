package com.fortrow.cxnfs.manifest;

import com.fortrow.cxnfs.hash.ContentHash;

public record ChunkRef(
        ContentHash hash,
        int size
) {
    public ChunkRef {
        if (hash == null) {
            throw new IllegalArgumentException("hash is required");
        }
        if (size < 0) {
            throw new IllegalArgumentException("size must be non-negative");
        }
    }
}
