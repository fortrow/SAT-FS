package com.fortrow.cxnfs.transport;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class PayloadStagingCleanerTest {
    @TempDir
    Path tempDir;

    @Test
    void inspectsEligibleSpillFilesWithoutDeletingThem() throws Exception {
        Path oldStage = writeStageFile("old.cxnfs-stage", "old payload", Instant.parse("2026-05-30T12:00:00Z"));
        Path freshStage = writeStageFile("fresh.cxnfs-stage", "fresh payload", Instant.parse("2026-06-01T11:59:00Z"));
        writeStageFile("not-a-stage.tmp", "ignore", Instant.parse("2026-05-30T12:00:00Z"));
        PayloadStagingCleaner cleaner = new PayloadStagingCleaner(fixedClock());

        PayloadStagingCleanupResult result = cleaner.inspect(tempDir, Duration.ofHours(1));

        assertTrue(result.dryRun());
        assertEquals(2, result.scannedStageFiles());
        assertEquals(1, result.eligibleStageFiles());
        assertEquals(Files.size(oldStage), result.eligibleStageBytes());
        assertEquals(0, result.removedStageFiles());
        assertEquals(0, result.reclaimedBytes());
        assertTrue(Files.exists(oldStage));
        assertTrue(Files.exists(freshStage));
        assertTrue(result.toReportText().contains("cxnfs.payload-staging.cleanup.v1"));
    }

    @Test
    void deletesOnlyEligibleSpillFilesWhenNotDryRun() throws Exception {
        Path oldStage = writeStageFile("old.cxnfs-stage", "old payload", Instant.parse("2026-05-30T12:00:00Z"));
        Path freshStage = writeStageFile("fresh.cxnfs-stage", "fresh payload", Instant.parse("2026-06-01T11:59:00Z"));
        PayloadStagingCleaner cleaner = new PayloadStagingCleaner(fixedClock());

        PayloadStagingCleanupResult result = cleaner.clean(tempDir, Duration.ofHours(1), false);

        assertEquals(2, result.scannedStageFiles());
        assertEquals(1, result.eligibleStageFiles());
        assertEquals(1, result.removedStageFiles());
        assertEquals("old payload".getBytes(StandardCharsets.UTF_8).length, result.reclaimedBytes());
        assertTrue(Files.notExists(oldStage));
        assertTrue(Files.exists(freshStage));
    }

    private Path writeStageFile(String name, String content, Instant lastModified) throws Exception {
        Path path = tempDir.resolve(name);
        Files.writeString(path, content, StandardCharsets.UTF_8);
        Files.setLastModifiedTime(path, FileTime.from(lastModified));
        return path;
    }

    private static Clock fixedClock() {
        return Clock.fixed(Instant.parse("2026-06-01T12:00:00Z"), ZoneId.of("UTC"));
    }
}
