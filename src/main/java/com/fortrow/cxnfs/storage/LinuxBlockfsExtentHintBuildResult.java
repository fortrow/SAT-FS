package com.fortrow.cxnfs.storage;

public record LinuxBlockfsExtentHintBuildResult(
        long generation,
        int manifestFiles,
        int manifestChunks,
        int allocations,
        int extentHints,
        boolean complete
) {
    public LinuxBlockfsExtentHintBuildResult {
        if (generation < 0) {
            throw new IllegalArgumentException("generation must be non-negative");
        }
        if (manifestFiles < 0) {
            throw new IllegalArgumentException("manifestFiles must be non-negative");
        }
        if (manifestChunks < 0) {
            throw new IllegalArgumentException("manifestChunks must be non-negative");
        }
        if (allocations < 0) {
            throw new IllegalArgumentException("allocations must be non-negative");
        }
        if (extentHints < 0) {
            throw new IllegalArgumentException("extentHints must be non-negative");
        }
    }
}
