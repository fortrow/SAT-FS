package com.fortrow.cxnfs.performance;

public record PerformanceObjective(
        String baselineName,
        double bottleneckGigabitsPerSecond,
        double minimumLinkUtilization,
        double minimumBaselineImprovement,
        long minimumReadBytesPerSecond
) {
    public static PerformanceObjective beatLegacyMountedShareOn25GigabitDatasetReads() {
        double bottleneckGigabitsPerSecond = 2.5;
        double minimumLinkUtilization = 0.85;
        long minimumReadBytesPerSecond = (long) ((bottleneckGigabitsPerSecond * 1_000_000_000L / 8.0)
                * minimumLinkUtilization);
        return new PerformanceObjective(
                "legacy-mounted-share",
                bottleneckGigabitsPerSecond,
                minimumLinkUtilization,
                0.05,
                minimumReadBytesPerSecond
        );
    }

    public PerformanceObjective {
        if (baselineName == null || baselineName.isBlank()) {
            throw new IllegalArgumentException("baselineName is required");
        }
        if (bottleneckGigabitsPerSecond <= 0) {
            throw new IllegalArgumentException("bottleneckGigabitsPerSecond must be positive");
        }
        if (minimumLinkUtilization <= 0 || minimumLinkUtilization > 1) {
            throw new IllegalArgumentException("minimumLinkUtilization must be between 0 and 1");
        }
        if (minimumBaselineImprovement < 0) {
            throw new IllegalArgumentException("minimumBaselineImprovement must be non-negative");
        }
        if (minimumReadBytesPerSecond <= 0) {
            throw new IllegalArgumentException("minimumReadBytesPerSecond must be positive");
        }
    }
}
