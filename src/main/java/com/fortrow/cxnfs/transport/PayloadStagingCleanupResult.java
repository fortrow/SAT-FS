package com.fortrow.cxnfs.transport;

import java.nio.file.Path;
import java.time.Duration;

public record PayloadStagingCleanupResult(
        Path spillDirectory,
        boolean dryRun,
        Duration minAge,
        int scannedStageFiles,
        int eligibleStageFiles,
        long eligibleStageBytes,
        int removedStageFiles,
        long removedStageBytes
) {
    public PayloadStagingCleanupResult {
        if (spillDirectory == null) {
            throw new IllegalArgumentException("spillDirectory is required");
        }
        if (minAge == null) {
            throw new IllegalArgumentException("minAge is required");
        }
        if (minAge.isNegative()) {
            throw new IllegalArgumentException("minAge must be non-negative");
        }
        if (scannedStageFiles < 0
                || eligibleStageFiles < 0
                || eligibleStageBytes < 0
                || removedStageFiles < 0
                || removedStageBytes < 0) {
            throw new IllegalArgumentException("cleanup counts must be non-negative");
        }
        spillDirectory = spillDirectory.toAbsolutePath().normalize();
    }

    public long reclaimedBytes() {
        return removedStageBytes;
    }

    public String toReportText() {
        String lineSeparator = System.lineSeparator();
        return "cxnfs.payload-staging.cleanup.v1" + lineSeparator
                + "spill_directory=" + spillDirectory + lineSeparator
                + "dry_run=" + dryRun + lineSeparator
                + "min_age_ms=" + minAge.toMillis() + lineSeparator
                + "scanned_stage_files=" + scannedStageFiles + lineSeparator
                + "eligible_stage_files=" + eligibleStageFiles + lineSeparator
                + "eligible_stage_bytes=" + eligibleStageBytes + lineSeparator
                + "removed_stage_files=" + removedStageFiles + lineSeparator
                + "removed_stage_bytes=" + removedStageBytes + lineSeparator
                + "reclaimed_bytes=" + reclaimedBytes() + lineSeparator;
    }
}
