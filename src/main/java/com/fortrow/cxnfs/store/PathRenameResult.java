package com.fortrow.cxnfs.store;

import com.fortrow.cxnfs.manifest.FileManifest;
import com.fortrow.cxnfs.metadata.FileMetadata;

import java.util.List;

public record PathRenameResult(
        String sourcePath,
        String targetPath,
        FileMetadata metadata,
        List<FileManifest> updatedManifests
) {
    public PathRenameResult {
        if (sourcePath == null || sourcePath.isBlank()) {
            throw new IllegalArgumentException("sourcePath is required");
        }
        if (targetPath == null || targetPath.isBlank()) {
            throw new IllegalArgumentException("targetPath is required");
        }
        if (metadata == null) {
            throw new IllegalArgumentException("metadata is required");
        }
        if (updatedManifests == null) {
            throw new IllegalArgumentException("updatedManifests is required");
        }
        updatedManifests = List.copyOf(updatedManifests);
    }
}
