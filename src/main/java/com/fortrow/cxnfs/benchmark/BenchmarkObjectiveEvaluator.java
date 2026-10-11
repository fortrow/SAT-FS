package com.fortrow.cxnfs.benchmark;

import com.fortrow.cxnfs.performance.PerformanceObjective;

public final class BenchmarkObjectiveEvaluator {
    private BenchmarkObjectiveEvaluator() {
    }

    public static BenchmarkObjectiveEvaluation evaluate(
            String objectiveName,
            BenchmarkComparison comparison,
            PerformanceObjective objective
    ) {
        if (comparison == null) {
            throw new IllegalArgumentException("comparison is required");
        }
        if (objective == null) {
            throw new IllegalArgumentException("objective is required");
        }
        double bottleneckBytesPerSecond = objective.bottleneckGigabitsPerSecond() * 1_000_000_000.0 / 8.0;
        double candidateLinkUtilization = comparison.candidate().bytesPerSecond() / bottleneckBytesPerSecond;
        return new BenchmarkObjectiveEvaluation(
                objectiveName,
                comparison,
                objective,
                candidateLinkUtilization,
                candidateLinkUtilization >= objective.minimumLinkUtilization(),
                comparison.candidate().bytesPerSecond() >= objective.minimumReadBytesPerSecond(),
                comparison.improvementRatio() >= objective.minimumBaselineImprovement()
        );
    }
}
