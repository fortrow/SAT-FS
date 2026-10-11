package com.fortrow.cxnfs.manifest;

import com.fortrow.cxnfs.hash.ContentHash;

import java.util.List;

public record FileManifest(
        String path,
        ContentHash fileHash,
        long size,
        List<ChunkRef> chunks
) {
    public FileManifest {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("path is required");
        }
        if (fileHash == null) {
            throw new IllegalArgumentException("fileHash is required");
        }
        if (size < 0) {
            throw new IllegalArgumentException("size must be non-negative");
        }
        chunks = List.copyOf(chunks);
    }
}
