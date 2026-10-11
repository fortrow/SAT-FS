package com.fortrow.cxnfs.benchmark;

import java.util.Locale;

public final class BenchmarkReportFormatter {
    private BenchmarkReportFormatter() {
    }

    public static String format(BenchmarkResult result) {
        return String.format(
                Locale.ROOT,
                "%s %s iterations=%d warmup=%d block=%s bytes=%d seconds=%.3f MiB/s=%.2f MB/s=%.2f Gbit/s=%.3f",
                result.label(),
                result.path(),
                result.iterations(),
                result.warmupIterations(),
                humanBytes(result.blockSizeBytes()),
                result.bytesRead(),
                result.seconds(),
                result.mebibytesPerSecond(),
                result.megabytesPerSecond(),
                result.gigabitsPerSecond()
        );
    }

    public static String format(BenchmarkComparison comparison) {
        return format(comparison.baseline())
                + System.lineSeparator()
                + format(comparison.candidate())
                + System.lineSeparator()
                + String.format(
                Locale.ROOT,
                "candidate_vs_baseline improvement=%.2f%% faster=%s",
                comparison.improvementRatio() * 100.0,
                comparison.candidateIsFaster()
        );
    }

    public static String format(BenchmarkObjectiveEvaluation evaluation) {
        return String.format(
                Locale.ROOT,
                "objective=%s baseline=%s bottleneck_gbit_s=%.3f minimum_link_utilization=%.3f "
                        + "candidate_link_utilization=%.3f minimum_candidate_MB_s=%.2f "
                        + "candidate_MB_s=%.2f minimum_baseline_improvement=%.2f%% "
                        + "candidate_improvement=%.2f%% link_passed=%s throughput_passed=%s "
                        + "improvement_passed=%s objective_passed=%s",
                evaluation.objectiveName(),
                evaluation.objective().baselineName(),
                evaluation.objective().bottleneckGigabitsPerSecond(),
                evaluation.objective().minimumLinkUtilization(),
                evaluation.candidateLinkUtilization(),
                evaluation.objective().minimumReadBytesPerSecond() / 1_000_000.0,
                evaluation.comparison().candidate().megabytesPerSecond(),
                evaluation.objective().minimumBaselineImprovement() * 100.0,
                evaluation.comparison().improvementRatio() * 100.0,
                evaluation.linkUtilizationPassed(),
                evaluation.minimumThroughputPassed(),
                evaluation.baselineImprovementPassed(),
                evaluation.passed()
        );
    }

    public static String humanBytes(long bytes) {
        if (bytes % (1024L * 1024L * 1024L) == 0 && bytes >= 1024L * 1024L * 1024L) {
            return (bytes / (1024L * 1024L * 1024L)) + "GiB";
        }
        if (bytes % (1024L * 1024L) == 0 && bytes >= 1024L * 1024L) {
            return (bytes / (1024L * 1024L)) + "MiB";
        }
        if (bytes % 1024L == 0 && bytes >= 1024L) {
            return (bytes / 1024L) + "KiB";
        }
        return Long.toString(bytes);
    }
}
