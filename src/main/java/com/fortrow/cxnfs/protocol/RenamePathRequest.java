package com.fortrow.cxnfs.protocol;

public record RenamePathRequest(
        String sourcePath,
        String targetPath
) implements ControlPayload {
    public RenamePathRequest {
        if (sourcePath == null || sourcePath.isBlank()) {
            throw new IllegalArgumentException("sourcePath is required");
        }
        if (targetPath == null || targetPath.isBlank()) {
            throw new IllegalArgumentException("targetPath is required");
        }
    }

    @Override
    public ControlOperation operation() {
        return ControlOperation.RENAME_PATH;
    }
}
