package com.fortrow.cxnfs.metadata;

import com.fortrow.cxnfs.hash.ContentHash;

import java.time.Instant;
import java.util.Optional;

public record FileMetadata(
        String path,
        String name,
        FileType type,
        long size,
        Optional<ContentHash> contentHash,
        Instant modifiedAt
) {
    public FileMetadata {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("path is required");
        }
        if (name == null) {
            throw new IllegalArgumentException("name is required");
        }
        if (type == null) {
            throw new IllegalArgumentException("type is required");
        }
        if (size < 0) {
            throw new IllegalArgumentException("size must be non-negative");
        }
        contentHash = contentHash == null ? Optional.empty() : contentHash;
        modifiedAt = modifiedAt == null ? Instant.EPOCH : modifiedAt;
    }
}
