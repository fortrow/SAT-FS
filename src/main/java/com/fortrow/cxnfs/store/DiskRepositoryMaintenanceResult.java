package com.fortrow.cxnfs.store;

import java.nio.file.Path;

public record DiskRepositoryMaintenanceResult(
        Path root,
        boolean dryRun,
        int manifestFiles,
        int referencedChunks,
        int scannedContainers,
        int removedContainers,
        long removedContainerBytes,
        int retainedYoungContainers,
        long orphanContainerMinAgeMillis,
        int removedTempFiles,
        long removedTempBytes,
        int retainedYoungTempFiles,
        long tempFileMinAgeMillis
) {
    public DiskRepositoryMaintenanceResult {
        if (root == null) {
            throw new IllegalArgumentException("root is required");
        }
        if (manifestFiles < 0) {
            throw new IllegalArgumentException("manifestFiles must be non-negative");
        }
        if (referencedChunks < 0) {
            throw new IllegalArgumentException("referencedChunks must be non-negative");
        }
        if (scannedContainers < 0) {
            throw new IllegalArgumentException("scannedContainers must be non-negative");
        }
        if (removedContainers < 0) {
            throw new IllegalArgumentException("removedContainers must be non-negative");
        }
        if (removedContainerBytes < 0) {
            throw new IllegalArgumentException("removedContainerBytes must be non-negative");
        }
        if (retainedYoungContainers < 0) {
            throw new IllegalArgumentException("retainedYoungContainers must be non-negative");
        }
        if (orphanContainerMinAgeMillis < 0) {
            throw new IllegalArgumentException("orphanContainerMinAgeMillis must be non-negative");
        }
        if (removedTempFiles < 0) {
            throw new IllegalArgumentException("removedTempFiles must be non-negative");
        }
        if (removedTempBytes < 0) {
            throw new IllegalArgumentException("removedTempBytes must be non-negative");
        }
        if (retainedYoungTempFiles < 0) {
            throw new IllegalArgumentException("retainedYoungTempFiles must be non-negative");
        }
        if (tempFileMinAgeMillis < 0) {
            throw new IllegalArgumentException("tempFileMinAgeMillis must be non-negative");
        }
    }

    public long removedBytes() {
        return removedContainerBytes + removedTempBytes;
    }

    public String toReportText() {
        String lineSeparator = System.lineSeparator();
        return "cxnfs.repository.maintenance.v1" + lineSeparator
                + "root=" + root + lineSeparator
                + "dry_run=" + dryRun + lineSeparator
                + "manifest_files=" + manifestFiles + lineSeparator
                + "referenced_chunks=" + referencedChunks + lineSeparator
                + "scanned_containers=" + scannedContainers + lineSeparator
                + "removed_containers=" + removedContainers + lineSeparator
                + "removed_container_bytes=" + removedContainerBytes + lineSeparator
                + "retained_young_containers=" + retainedYoungContainers + lineSeparator
                + "orphan_container_min_age_ms=" + orphanContainerMinAgeMillis + lineSeparator
                + "removed_temp_files=" + removedTempFiles + lineSeparator
                + "removed_temp_bytes=" + removedTempBytes + lineSeparator
                + "retained_young_temp_files=" + retainedYoungTempFiles + lineSeparator
                + "temp_file_min_age_ms=" + tempFileMinAgeMillis + lineSeparator
                + "removed_bytes=" + removedBytes() + lineSeparator;
    }
}
