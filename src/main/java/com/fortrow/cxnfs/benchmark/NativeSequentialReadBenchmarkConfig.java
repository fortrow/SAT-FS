package com.fortrow.cxnfs.benchmark;

import com.fortrow.cxnfs.protocol.TransferPayloadFormat;

public record NativeSequentialReadBenchmarkConfig(
        String label,
        String path,
        long offset,
        long length,
        int iterations,
        int warmupIterations,
        int blockSizeBytes,
        TransferPayloadFormat payloadFormat
) {
    public static final long READ_TO_EOF = -1L;

    public static NativeSequentialReadBenchmarkConfig defaults(String label, String path) {
        return new NativeSequentialReadBenchmarkConfig(
                label,
                path,
                0,
                READ_TO_EOF,
                SequentialReadBenchmarkConfig.DEFAULT_ITERATIONS,
                SequentialReadBenchmarkConfig.DEFAULT_WARMUP_ITERATIONS,
                SequentialReadBenchmarkConfig.DEFAULT_BLOCK_SIZE_BYTES,
                TransferPayloadFormat.MDS_SHARDS
        );
    }

    public NativeSequentialReadBenchmarkConfig {
        if (label == null || label.isBlank()) {
            throw new IllegalArgumentException("label is required");
        }
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("path is required");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be non-negative");
        }
        if (length < READ_TO_EOF) {
            throw new IllegalArgumentException("length must be non-negative or READ_TO_EOF");
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
        if (payloadFormat == null) {
            throw new IllegalArgumentException("payloadFormat is required");
        }
    }
}
