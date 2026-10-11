package com.fortrow.cxnfs.dataset;

import java.util.List;

public record MdsShardPlanEntry(
        String shardName,
        List<DatasetSourceFile> sourceFiles,
        long sizeBytes
) {
    public MdsShardPlanEntry {
        if (shardName == null || shardName.isBlank()) {
            throw new IllegalArgumentException("shardName is required");
        }
        sourceFiles = sourceFiles == null ? List.of() : List.copyOf(sourceFiles);
        if (sourceFiles.isEmpty()) {
            throw new IllegalArgumentException("sourceFiles are required");
        }
        if (sizeBytes < 0) {
            throw new IllegalArgumentException("sizeBytes must be non-negative");
        }
    }
}
