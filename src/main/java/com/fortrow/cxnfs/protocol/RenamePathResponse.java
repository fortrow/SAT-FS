package com.fortrow.cxnfs.protocol;

import com.fortrow.cxnfs.metadata.FileMetadata;

public record RenamePathResponse(
        String sourcePath,
        String targetPath,
        FileMetadata metadata
) implements ControlPayload {
    public RenamePathResponse {
        if (sourcePath == null || sourcePath.isBlank()) {
            throw new IllegalArgumentException("sourcePath is required");
        }
        if (targetPath == null || targetPath.isBlank()) {
            throw new IllegalArgumentException("targetPath is required");
        }
        if (metadata == null) {
            throw new IllegalArgumentException("metadata is required");
        }
    }

    @Override
    public ControlOperation operation() {
        return ControlOperation.RENAME_PATH_RESULT;
    }
}
