package com.fortrow.cxnfs.dataset;

import java.nio.file.Path;

public record MdsShardArtifact(
        String shardName,
        Path path,
        int sampleCount,
        long byteSize
) {
    public MdsShardArtifact {
        if (shardName == null || shardName.isBlank()) {
            throw new IllegalArgumentException("shardName is required");
        }
        if (path == null) {
            throw new IllegalArgumentException("path is required");
        }
        if (sampleCount < 0) {
            throw new IllegalArgumentException("sampleCount must be non-negative");
        }
        if (byteSize < 0) {
            throw new IllegalArgumentException("byteSize must be non-negative");
        }
    }
}
