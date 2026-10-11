package com.fortrow.cxnfs.dataset;

import java.util.List;

public final class DatasetFormatDecider {
    public DatasetFormatRecommendation recommend(DatasetUseCase useCase) {
        if (useCase == null) {
            throw new IllegalArgumentException("useCase is required");
        }

        return switch (useCase) {
            case PYTORCH_TRAINING_READS -> new DatasetFormatRecommendation(
                    useCase,
                    DatasetFormat.MDS_SHARDS,
                    List.of(DatasetFormat.PARQUET),
                    "Use Mosaic-style MDS-Shards for the hot training read path; keep Parquet as an analytics or import sidecar only.",
                    true
            );
            case COLUMNAR_ANALYTICS -> new DatasetFormatRecommendation(
                    useCase,
                    DatasetFormat.PARQUET,
                    List.of(DatasetFormat.MDS_SHARDS),
                    "Use Parquet for column pruning, typed tabular data, and interoperability with analytics engines; convert hot training splits to shards when loader latency matters.",
                    true
            );
            case CHECKPOINT_WRITES -> new DatasetFormatRecommendation(
                    useCase,
                    DatasetFormat.SAFETENSORS,
                    List.of(),
                    "Use Safetensors for model and tensor checkpoint writes, then send the output directory through the CXNFS dataset write path.",
                    true
            );
            case MIXED -> new DatasetFormatRecommendation(
                    useCase,
                    DatasetFormat.MDS_SHARDS,
                    List.of(DatasetFormat.PARQUET, DatasetFormat.SAFETENSORS),
                    "Use MDS-Shards for training reads, Parquet as the analytics sidecar, and Safetensors for checkpoint output writes.",
                    true
            );
        };
    }
}
