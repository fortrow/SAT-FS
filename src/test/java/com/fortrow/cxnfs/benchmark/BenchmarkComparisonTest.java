package com.fortrow.cxnfs.benchmark;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class BenchmarkComparisonTest {
    @Test
    void computesCandidateImprovementOverBaseline() {
        BenchmarkResult baseline = new BenchmarkResult("legacy-share", Path.of("/legacy"), 1, 0, 1024, 1000, 1_000_000_000);
        BenchmarkResult candidate = new BenchmarkResult("cxnfs", Path.of("/cxnfs"), 1, 0, 1024, 1200, 1_000_000_000);

        BenchmarkComparison comparison = new BenchmarkComparison(baseline, candidate);

        assertTrue(comparison.candidateIsFaster());
        assertEquals(0.2, comparison.improvementRatio(), 0.0001);
    }
}
