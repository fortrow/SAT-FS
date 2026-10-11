package com.fortrow.cxnfs.benchmark;

import com.fortrow.cxnfs.performance.PerformanceObjective;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class BenchmarkReportFormatterTest {
    @Test
    void formatsHumanBlockSizes() {
        assertEquals("8MiB", BenchmarkReportFormatter.humanBytes(8 * 1024 * 1024));
        assertEquals("64KiB", BenchmarkReportFormatter.humanBytes(64 * 1024));
        assertEquals("7", BenchmarkReportFormatter.humanBytes(7));
    }

    @Test
    void formatsResultWithThroughputUnits() {
        BenchmarkResult result = new BenchmarkResult("legacy-share", Path.of("/mnt/legacy/file"), 1, 0, 1024, 1024 * 1024, 1_000_000_000);

        String report = BenchmarkReportFormatter.format(result);

        assertTrue(report.contains("legacy-share"));
        assertTrue(report.contains("MiB/s=1.00"));
        assertTrue(report.contains("Gbit/s=0.008"));
    }

    @Test
    void formatsObjectiveEvaluationWithPassFailFields() {
        BenchmarkComparison comparison = new BenchmarkComparison(
                new BenchmarkResult("legacy-share", Path.of("/legacy"), 1, 0, 1024, 250_000_000L, 1_000_000_000),
                new BenchmarkResult("cxnfs", Path.of("/cxnfs"), 1, 0, 1024, 270_000_000L, 1_000_000_000)
        );
        BenchmarkObjectiveEvaluation evaluation = BenchmarkObjectiveEvaluator.evaluate(
                "ai-training-25gbe",
                comparison,
                PerformanceObjective.beatLegacyMountedShareOn25GigabitDatasetReads()
        );

        String report = BenchmarkReportFormatter.format(evaluation);

        assertTrue(report.contains("objective=ai-training-25gbe"));
        assertTrue(report.contains("baseline=legacy-mounted-share"));
        assertTrue(report.contains("link_passed=true"));
        assertTrue(report.contains("throughput_passed=true"));
        assertTrue(report.contains("improvement_passed=true"));
        assertTrue(report.contains("objective_passed=true"));
    }
}
