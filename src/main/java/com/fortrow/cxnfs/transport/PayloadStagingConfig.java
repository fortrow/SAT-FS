package com.fortrow.cxnfs.transport;

import java.nio.file.Path;
import java.util.Optional;

public record PayloadStagingConfig(
        long maxMemoryBytes,
        int maxSinglePayloadBytes,
        Optional<Path> spillDirectory
) {
    public static final long DEFAULT_MAX_MEMORY_BYTES = 128L * 1024 * 1024;

    public static PayloadStagingConfig memoryOnly(long maxMemoryBytes) {
        return new PayloadStagingConfig(maxMemoryBytes, BulkDataFrameCodec.MAX_PAYLOAD_BYTES, Optional.empty());
    }

    public static PayloadStagingConfig withSpill(long maxMemoryBytes, Path spillDirectory) {
        return new PayloadStagingConfig(maxMemoryBytes, BulkDataFrameCodec.MAX_PAYLOAD_BYTES, Optional.of(spillDirectory));
    }

    public static PayloadStagingConfig serverDefault() {
        return withSpill(DEFAULT_MAX_MEMORY_BYTES, defaultSpillDirectory());
    }

    public static Path defaultSpillDirectory() {
        return Path.of(System.getProperty("java.io.tmpdir"), "cxnfs-payload-staging")
                .toAbsolutePath()
                .normalize();
    }

    public PayloadStagingConfig {
        if (maxMemoryBytes < 0) {
            throw new IllegalArgumentException("maxMemoryBytes must be non-negative");
        }
        if (maxSinglePayloadBytes <= 0 || maxSinglePayloadBytes > BulkDataFrameCodec.MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("maxSinglePayloadBytes must be between 1 and " + BulkDataFrameCodec.MAX_PAYLOAD_BYTES);
        }
        spillDirectory = spillDirectory == null ? Optional.empty() : spillDirectory;
        spillDirectory = spillDirectory.map(path -> {
            if (path == null) {
                throw new IllegalArgumentException("spillDirectory must not contain null");
            }
            return path.toAbsolutePath().normalize();
        });
    }
}
