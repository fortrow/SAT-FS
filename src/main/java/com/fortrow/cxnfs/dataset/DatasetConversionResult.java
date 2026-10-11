package com.fortrow.cxnfs.dataset;

import java.nio.file.Path;
import java.util.List;

public record DatasetConversionResult(
        Path outputDirectory,
        Path indexPath,
        int shardCount,
        long sampleCount,
        long bytesWritten,
        List<MdsShardArtifact> shards
) {
    public DatasetConversionResult {
        if (outputDirectory == null) {
            throw new IllegalArgumentException("outputDirectory is required");
        }
        if (indexPath == null) {
            throw new IllegalArgumentException("indexPath is required");
        }
        if (shardCount < 0) {
            throw new IllegalArgumentException("shardCount must be non-negative");
        }
        if (sampleCount < 0) {
            throw new IllegalArgumentException("sampleCount must be non-negative");
        }
        if (bytesWritten < 0) {
            throw new IllegalArgumentException("bytesWritten must be non-negative");
        }
        shards = shards == null ? List.of() : List.copyOf(shards);
        if (shardCount != shards.size()) {
            throw new IllegalArgumentException("shardCount must match shards");
        }
    }
}
