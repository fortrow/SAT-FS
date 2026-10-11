package com.fortrow.cxnfs.storage;

public record LinuxBlockfsPublishResult(
        int attemptedExtents,
        int acceptedExtents,
        int unsupportedExtents,
        long acceptedBytes
) {
    public LinuxBlockfsPublishResult {
        if (attemptedExtents < 0) {
            throw new IllegalArgumentException("attemptedExtents must be non-negative");
        }
        if (acceptedExtents < 0) {
            throw new IllegalArgumentException("acceptedExtents must be non-negative");
        }
        if (unsupportedExtents < 0) {
            throw new IllegalArgumentException("unsupportedExtents must be non-negative");
        }
        if (acceptedBytes < 0) {
            throw new IllegalArgumentException("acceptedBytes must be non-negative");
        }
        if (acceptedExtents + unsupportedExtents > attemptedExtents) {
            throw new IllegalArgumentException("accepted plus unsupported cannot exceed attempts");
        }
    }

    public boolean fullyAccepted() {
        return attemptedExtents > 0
                && acceptedExtents == attemptedExtents
                && unsupportedExtents == 0;
    }
}
