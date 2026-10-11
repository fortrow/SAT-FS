package com.fortrow.cxnfs.benchmark;

import java.nio.file.Path;

public record SequentialReadBenchmarkConfig(
        String label,
        Path path,
        int iterations,
        int warmupIterations,
        int blockSizeBytes
) {
    public static final int DEFAULT_ITERATIONS = 3;
    public static final int DEFAULT_WARMUP_ITERATIONS = 1;
    public static final int DEFAULT_BLOCK_SIZE_BYTES = 8 * 1024 * 1024;

    public static SequentialReadBenchmarkConfig defaults(String label, Path path) {
        return new SequentialReadBenchmarkConfig(
                label,
                path,
                DEFAULT_ITERATIONS,
                DEFAULT_WARMUP_ITERATIONS,
                DEFAULT_BLOCK_SIZE_BYTES
        );
    }

    public SequentialReadBenchmarkConfig {
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
    }
}
