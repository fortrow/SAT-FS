package com.fortrow.cxnfs.performance;

import com.fortrow.cxnfs.transport.CordPoolConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class LinuxDatasetPerformanceProfileTest {
    @Test
    void aiTrainingProfileTargetsLegacyMountedShareOn25GigabitBottleneck() {
        LinuxDatasetPerformanceProfile profile = LinuxDatasetPerformanceProfile.aiTraining25Gigabit();

        assertEquals(WorkloadProfile.AI_TRAINING_DATASET, profile.workload());
        assertEquals(CordPoolConfig.ABSOLUTE_MAX_CORDS, profile.cordPool().maxCords());
        assertEquals("legacy-mounted-share", profile.objective().baselineName());
        assertEquals(2.5, profile.objective().bottleneckGigabitsPerSecond());
        assertEquals(0.85, profile.objective().minimumLinkUtilization());
        assertEquals(0.05, profile.objective().minimumBaselineImprovement());
        assertEquals(265_625_000L, profile.objective().minimumReadBytesPerSecond());
    }

    @Test
    void aiTrainingProfilePrefersLinuxReadPathOptimizations() {
        LinuxDatasetPerformanceProfile profile = LinuxDatasetPerformanceProfile.aiTraining25Gigabit();

        assertTrue(profile.kernelPageCachePreferred());
        assertTrue(profile.directBuffersPreferred());
        assertEquals(32L * 1024 * 1024, profile.readAheadPolicy().forwardWindowBytes());
        assertEquals(512L * 1024 * 1024, profile.readAheadPolicy().maxPrefetchBytes());
        assertEquals(1024L * 1024 * 1024, profile.backpressurePolicy().maxBytesInFlight());
    }
}
