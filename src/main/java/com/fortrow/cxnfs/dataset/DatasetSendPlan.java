package com.fortrow.cxnfs.dataset;

import java.nio.file.Path;

public record DatasetSendPlan(Path outputDirectory, DatasetFormat format, boolean nativePythonAccess) {
    public DatasetSendPlan {
        if (outputDirectory == null) {
            throw new IllegalArgumentException("outputDirectory is required");
        }
        if (format == null) {
            throw new IllegalArgumentException("format is required");
        }
        if (!format.isCheckpointWriteFormat()) {
            throw new IllegalArgumentException("send currently supports checkpoint write formats only: safetensors");
        }
    }

    public static DatasetSendPlan safetensors(Path outputDirectory) {
        return new DatasetSendPlan(outputDirectory, DatasetFormat.SAFETENSORS, true);
    }
}
