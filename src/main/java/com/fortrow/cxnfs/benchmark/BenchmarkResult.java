package com.fortrow.cxnfs.benchmark;

import java.nio.file.Path;

public record BenchmarkResult(
        String label,
        Path path,
        int iterations,
        int warmupIterations,
        int blockSizeBytes,
        long bytesRead,
        long elapsedNanos
) {
    public BenchmarkResult {
        if (label == null || label.isBlank()) {
            throw new IllegalArgumentException("label is required");
        }
        if (path == null) {
            throw new IllegalArgumentException("path is required");
        }
        if (iterations <= 0) {
            throw new IllegalArgumentException("iterations must be positive");
        }
        if (warmupIterations < 0) {
            throw new IllegalArgumentException("warmupIterations must be non-negative");
        }
        if (blockSizeBytes <= 0) {
            throw new IllegalArgumentException("blockSizeBytes must be positive");
        }
        if (bytesRead < 0) {
            throw new IllegalArgumentException("bytesRead must be non-negative");
        }
        if (elapsedNanos <= 0) {
            throw new IllegalArgumentException("elapsedNanos must be positive");
        }
    }

    public double seconds() {
        return elapsedNanos / 1_000_000_000.0;
    }

    public double bytesPerSecond() {
        return bytesRead / seconds();
    }

    public double mebibytesPerSecond() {
        return bytesPerSecond() / (1024.0 * 1024.0);
    }

    public double megabytesPerSecond() {
        return bytesPerSecond() / 1_000_000.0;
    }

    public double gigabitsPerSecond() {
        return bytesPerSecond() * 8.0 / 1_000_000_000.0;
    }
}
