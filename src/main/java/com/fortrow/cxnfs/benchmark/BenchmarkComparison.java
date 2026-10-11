package com.fortrow.cxnfs.benchmark;

public record BenchmarkComparison(
        BenchmarkResult baseline,
        BenchmarkResult candidate
) {
    public BenchmarkComparison {
        if (baseline == null) {
            throw new IllegalArgumentException("baseline is required");
        }
        if (candidate == null) {
            throw new IllegalArgumentException("candidate is required");
        }
    }

    public double improvementRatio() {
        return candidate.bytesPerSecond() / baseline.bytesPerSecond() - 1.0;
    }

    public boolean candidateIsFaster() {
        return improvementRatio() > 0;
    }
}
