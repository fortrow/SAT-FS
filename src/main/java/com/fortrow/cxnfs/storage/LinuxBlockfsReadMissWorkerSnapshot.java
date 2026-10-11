package com.fortrow.cxnfs.storage;

import java.time.Instant;
import java.util.Optional;

public record LinuxBlockfsReadMissWorkerSnapshot(
        boolean running,
        long drainCycles,
        long eventsResolved,
        long plannedChunks,
        long fetchedChunks,
        long publishedExtents,
        long unsupportedExtents,
        long payloadBytesWritten,
        Optional<Instant> lastDrainAt,
        Optional<String> lastError
) {
    public LinuxBlockfsReadMissWorkerSnapshot {
        if (drainCycles < 0 || eventsResolved < 0 || plannedChunks < 0 || fetchedChunks < 0
                || publishedExtents < 0 || unsupportedExtents < 0 || payloadBytesWritten < 0) {
            throw new IllegalArgumentException("counters must be non-negative");
        }
        lastDrainAt = lastDrainAt == null ? Optional.empty() : lastDrainAt;
        lastError = lastError == null ? Optional.empty() : lastError;
    }
}
