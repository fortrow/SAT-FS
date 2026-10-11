package com.fortrow.cxnfs.performance;

import com.fortrow.cxnfs.transport.CordPoolConfig;
import com.fortrow.cxnfs.transport.TransferBackpressurePolicy;

public record LinuxDatasetPerformanceProfile(
        WorkloadProfile workload,
        CordPoolConfig cordPool,
        TransferBackpressurePolicy backpressurePolicy,
        ReadAheadPolicy readAheadPolicy,
        PerformanceObjective objective,
        boolean kernelPageCachePreferred,
        boolean directBuffersPreferred,
        int hashWorkers,
        int decodeWorkers
) {
    public static LinuxDatasetPerformanceProfile aiTraining25Gigabit() {
        return new LinuxDatasetPerformanceProfile(
                WorkloadProfile.AI_TRAINING_DATASET,
                new CordPoolConfig(1, CordPoolConfig.ABSOLUTE_MAX_CORDS, true),
                new TransferBackpressurePolicy(
                        256,
                        1024L * 1024 * 1024,
                        100,
                        220L * 1024 * 1024
                ),
                ReadAheadPolicy.linuxAiTrainingDataset(),
                PerformanceObjective.beatLegacyMountedShareOn25GigabitDatasetReads(),
                true,
                true,
                4,
                4
        );
    }

    public LinuxDatasetPerformanceProfile {
        if (workload == null) {
            throw new IllegalArgumentException("workload is required");
        }
        if (cordPool == null) {
            throw new IllegalArgumentException("cordPool is required");
        }
        if (backpressurePolicy == null) {
            throw new IllegalArgumentException("backpressurePolicy is required");
        }
        if (readAheadPolicy == null) {
            throw new IllegalArgumentException("readAheadPolicy is required");
        }
        if (objective == null) {
            throw new IllegalArgumentException("objective is required");
        }
        if (hashWorkers <= 0) {
            throw new IllegalArgumentException("hashWorkers must be positive");
        }
        if (decodeWorkers <= 0) {
            throw new IllegalArgumentException("decodeWorkers must be positive");
        }
    }
}
