package com.fortrow.cxnfs.storage;

import java.nio.file.Path;

public record FilesystemStructureRef(
        FilesystemStructureRole role,
        Path path,
        FilesystemStructureTiming timing
) {
    public FilesystemStructureRef {
        if (role == null) {
            throw new IllegalArgumentException("role is required");
        }
        if (path == null) {
            throw new IllegalArgumentException("path is required");
        }
        if (timing == null) {
            throw new IllegalArgumentException("timing is required");
        }
        path = path.normalize();
    }

    public boolean requiredBeforePayload() {
        return timing == FilesystemStructureTiming.BEFORE_PAYLOAD;
    }

    public boolean trailing() {
        return timing == FilesystemStructureTiming.TRAILING;
    }
}
