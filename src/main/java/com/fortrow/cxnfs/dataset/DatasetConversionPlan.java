package com.fortrow.cxnfs.dataset;

import java.nio.file.Path;
import java.util.List;

public record DatasetConversionPlan(
        Path inputDirectory,
        String datasetName,
        DatasetSourceFormat sourceFormat,
        DatasetFormat targetFormat,
        long targetShardSizeBytes,
        List<DatasetSourceFile> sourceFiles,
        List<MdsShardPlanEntry> shards
) {
    public DatasetConversionPlan {
        if (inputDirectory == null) {
            throw new IllegalArgumentException("inputDirectory is required");
        }
        if (datasetName == null || datasetName.isBlank()) {
            throw new IllegalArgumentException("datasetName is required");
        }
        if (sourceFormat == null) {
            throw new IllegalArgumentException("sourceFormat is required");
        }
        if (targetFormat == null || targetFormat != DatasetFormat.MDS_SHARDS) {
            throw new IllegalArgumentException("targetFormat must be mds-shards");
        }
        if (targetShardSizeBytes <= 0) {
            throw new IllegalArgumentException("targetShardSizeBytes must be positive");
        }
        sourceFiles = sourceFiles == null ? List.of() : List.copyOf(sourceFiles);
        shards = shards == null ? List.of() : List.copyOf(shards);
        if (sourceFiles.isEmpty()) {
            throw new IllegalArgumentException("sourceFiles are required");
        }
        if (shards.isEmpty()) {
            throw new IllegalArgumentException("shards are required");
        }
    }

    public long totalSourceBytes() {
        return sourceFiles.stream().mapToLong(DatasetSourceFile::sizeBytes).sum();
    }
}
