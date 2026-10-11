package com.fortrow.cxnfs.dataset;

import java.nio.file.Path;

public record DatasetPreparePlan(
        Path inputDirectory,
        String datasetName,
        DatasetSourceFormat sourceFormat,
        DatasetFormat format,
        long targetShardSizeBytes,
        boolean nativePythonAccess
) {
    public static final long DEFAULT_TARGET_SHARD_SIZE_BYTES = 256L * 1024 * 1024;

    public DatasetPreparePlan {
        if (inputDirectory == null) {
            throw new IllegalArgumentException("inputDirectory is required");
        }
        if (datasetName == null || datasetName.isBlank()) {
            throw new IllegalArgumentException("datasetName is required");
        }
        if (sourceFormat == null) {
            throw new IllegalArgumentException("sourceFormat is required");
        }
        if (format == null) {
            throw new IllegalArgumentException("format is required");
        }
        if (!format.isDatasetReadFormat()) {
            throw new IllegalArgumentException("prepare supports the locked read format only: mds-shards");
        }
        if (targetShardSizeBytes <= 0) {
            throw new IllegalArgumentException("targetShardSizeBytes must be positive");
        }
    }

    public static DatasetPreparePlan mdsShards(Path inputDirectory, String datasetName) {
        return new DatasetPreparePlan(
                inputDirectory,
                datasetName,
                DatasetSourceFormat.FILES,
                DatasetFormat.MDS_SHARDS,
                DEFAULT_TARGET_SHARD_SIZE_BYTES,
                true
        );
    }
}
