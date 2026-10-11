package com.fortrow.cxnfs.dataset;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class DatasetFormatDeciderTest {
    private final DatasetFormatDecider decider = new DatasetFormatDecider();

    @Test
    void recommendsMdsShardsForPytorchTrainingReads() {
        DatasetFormatRecommendation recommendation = decider.recommend(DatasetUseCase.PYTORCH_TRAINING_READS);

        assertEquals(DatasetFormat.MDS_SHARDS, recommendation.primaryFormat());
        assertTrue(recommendation.companionFormats().contains(DatasetFormat.PARQUET));
        assertTrue(recommendation.preferNativePythonAccess());
    }

    @Test
    void recommendsSafetensorsForCheckpointWrites() {
        DatasetFormatRecommendation recommendation = decider.recommend(DatasetUseCase.CHECKPOINT_WRITES);

        assertEquals(DatasetFormat.SAFETENSORS, recommendation.primaryFormat());
        assertTrue(recommendation.companionFormats().isEmpty());
        assertTrue(recommendation.preferNativePythonAccess());
    }

    @Test
    void mixedWorkloadKeepsMdsShardsAsTheReadFormat() {
        DatasetFormatRecommendation recommendation = decider.recommend(DatasetUseCase.MIXED);

        assertEquals(DatasetFormat.MDS_SHARDS, recommendation.primaryFormat());
        assertTrue(recommendation.companionFormats().contains(DatasetFormat.PARQUET));
        assertTrue(recommendation.companionFormats().contains(DatasetFormat.SAFETENSORS));
    }
}
