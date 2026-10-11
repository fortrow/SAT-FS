package com.fortrow.cxnfs.transport;

import java.nio.file.Path;
import java.util.Optional;

public record StagedPayload(
        BulkDataFrameKey key,
        int cordIndex,
        long entryOffset,
        int payloadLength,
        boolean finalFrameForChunk,
        StagedPayloadStorage storage,
        Optional<Path> spillPath,
        boolean metadataVerified
) {
    public StagedPayload {
        if (key == null) {
            throw new IllegalArgumentException("key is required");
        }
        if (cordIndex < 0 || cordIndex >= CordPoolConfig.ABSOLUTE_MAX_CORDS) {
            throw new IllegalArgumentException("cordIndex must be between 0 and " + (CordPoolConfig.ABSOLUTE_MAX_CORDS - 1));
        }
        if (entryOffset < 0) {
            throw new IllegalArgumentException("entryOffset must be non-negative");
        }
        if (payloadLength <= 0) {
            throw new IllegalArgumentException("payloadLength must be positive");
        }
        if (storage == null) {
            throw new IllegalArgumentException("storage is required");
        }
        spillPath = spillPath == null ? Optional.empty() : spillPath;
        if (storage == StagedPayloadStorage.SPILL && spillPath.isEmpty()) {
            throw new IllegalArgumentException("spillPath is required for spilled payloads");
        }
        if (storage == StagedPayloadStorage.MEMORY && spillPath.isPresent()) {
            throw new IllegalArgumentException("memory payloads must not have a spillPath");
        }
    }

    public StagedPayload verified() {
        return new StagedPayload(key, cordIndex, entryOffset, payloadLength, finalFrameForChunk, storage, spillPath, true);
    }
}
