package com.fortrow.cxnfs.benchmark;

import com.fortrow.cxnfs.performance.PerformanceObjective;

public record BenchmarkObjectiveEvaluation(
        String objectiveName,
        BenchmarkComparison comparison,
        PerformanceObjective objective,
        double candidateLinkUtilization,
        boolean linkUtilizationPassed,
        boolean minimumThroughputPassed,
        boolean baselineImprovementPassed
) {
    public BenchmarkObjectiveEvaluation {
        if (objectiveName == null || objectiveName.isBlank()) {
            throw new IllegalArgumentException("objectiveName is required");
        }
        if (comparison == null) {
            throw new IllegalArgumentException("comparison is required");
        }
        if (objective == null) {
            throw new IllegalArgumentException("objective is required");
        }
        if (candidateLinkUtilization < 0) {
            throw new IllegalArgumentException("candidateLinkUtilization must be non-negative");
        }
    }

    public boolean passed() {
        return linkUtilizationPassed && minimumThroughputPassed && baselineImprovementPassed;
    }
}
