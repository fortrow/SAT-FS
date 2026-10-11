package com.fortrow.cxnfs.transport;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

public final class PayloadStagingCleaner {
    public static final String STAGE_SUFFIX = ".cxnfs-stage";
    public static final Duration DEFAULT_MIN_AGE = Duration.ofHours(24);

    private final Clock clock;

    public PayloadStagingCleaner() {
        this(Clock.systemUTC());
    }

    public PayloadStagingCleaner(Clock clock) {
        if (clock == null) {
            throw new IllegalArgumentException("clock is required");
        }
        this.clock = clock;
    }

    public PayloadStagingCleanupResult inspect(Path spillDirectory, Duration minAge) throws IOException {
        return clean(spillDirectory, minAge, true);
    }

    public PayloadStagingCleanupResult clean(Path spillDirectory, Duration minAge, boolean dryRun) throws IOException {
        if (spillDirectory == null) {
            throw new IllegalArgumentException("spillDirectory is required");
        }
        if (minAge == null) {
            throw new IllegalArgumentException("minAge is required");
        }
        if (minAge.isNegative()) {
            throw new IllegalArgumentException("minAge must be non-negative");
        }

        Path root = spillDirectory.toAbsolutePath().normalize();
        Files.createDirectories(root);
        Instant cutoff = clock.instant().minus(minAge);
        int scanned = 0;
        int eligible = 0;
        long eligibleBytes = 0;
        int removed = 0;
        long removedBytes = 0;

        for (Path path : stageFiles(root)) {
            scanned++;
            FileTime lastModified = Files.getLastModifiedTime(path);
            if (lastModified.toInstant().isAfter(cutoff)) {
                continue;
            }
            long bytes = Files.size(path);
            eligible++;
            eligibleBytes += bytes;
            if (!dryRun) {
                Files.delete(path);
                removed++;
                removedBytes += bytes;
            }
        }

        return new PayloadStagingCleanupResult(
                root,
                dryRun,
                minAge,
                scanned,
                eligible,
                eligibleBytes,
                removed,
                removedBytes
        );
    }

    private static List<Path> stageFiles(Path root) throws IOException {
        try (Stream<Path> paths = Files.list(root)) {
            return paths
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(STAGE_SUFFIX))
                    .sorted(Comparator.naturalOrder())
                    .toList();
        }
    }
}
