package com.fortrow.cxnfs.benchmark;

import com.fortrow.cxnfs.performance.PerformanceObjective;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class BenchmarkObjectiveEvaluatorTest {
    @Test
    void passesWhenCandidateMeetsLinkThroughputAndImprovementTargets() {
        BenchmarkComparison comparison = new BenchmarkComparison(
                result("legacy-share", 250_000_000L),
                result("cxnfs", 270_000_000L)
        );

        BenchmarkObjectiveEvaluation evaluation = BenchmarkObjectiveEvaluator.evaluate(
                "ai-training-25gbe",
                comparison,
                PerformanceObjective.beatLegacyMountedShareOn25GigabitDatasetReads()
        );

        assertTrue(evaluation.linkUtilizationPassed());
        assertTrue(evaluation.minimumThroughputPassed());
        assertTrue(evaluation.baselineImprovementPassed());
        assertTrue(evaluation.passed());
    }

    @Test
    void failsWhenCandidateMissesAnyRequiredTarget() {
        BenchmarkComparison comparison = new BenchmarkComparison(
                result("legacy-share", 255_000_000L),
                result("cxnfs", 260_000_000L)
        );

        BenchmarkObjectiveEvaluation evaluation = BenchmarkObjectiveEvaluator.evaluate(
                "ai-training-25gbe",
                comparison,
                PerformanceObjective.beatLegacyMountedShareOn25GigabitDatasetReads()
        );

        assertFalse(evaluation.linkUtilizationPassed());
        assertFalse(evaluation.minimumThroughputPassed());
        assertFalse(evaluation.baselineImprovementPassed());
        assertFalse(evaluation.passed());
    }

    private static BenchmarkResult result(String label, long bytesPerSecond) {
        return new BenchmarkResult(
                label,
                Path.of("/" + label),
                1,
                0,
                1024,
                bytesPerSecond,
                1_000_000_000
        );
    }
}
