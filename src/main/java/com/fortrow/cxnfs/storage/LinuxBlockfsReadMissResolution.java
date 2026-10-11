package com.fortrow.cxnfs.storage;

public record LinuxBlockfsReadMissResolution(
        LinuxBlockfsMissEvent event,
        int plannedChunks,
        int fetchedChunks,
        int publishedExtents,
        int unsupportedExtents,
        long bytesWritten
) {
    public LinuxBlockfsReadMissResolution {
        if (event == null) {
            throw new IllegalArgumentException("event is required");
        }
        if (plannedChunks < 0 || fetchedChunks < 0 || publishedExtents < 0
                || unsupportedExtents < 0 || bytesWritten < 0) {
            throw new IllegalArgumentException("counts and bytes must be non-negative");
        }
        if (fetchedChunks > plannedChunks) {
            throw new IllegalArgumentException("fetchedChunks cannot exceed plannedChunks");
        }
        if (publishedExtents + unsupportedExtents > fetchedChunks) {
            throw new IllegalArgumentException("published plus unsupported extents cannot exceed fetchedChunks");
        }
    }

    public boolean fullyPublished() {
        return plannedChunks == fetchedChunks
                && fetchedChunks == publishedExtents
                && unsupportedExtents == 0;
    }
}
