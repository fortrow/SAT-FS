package com.fortrow.cxnfs.protocol;

import com.fortrow.cxnfs.metadata.FileMetadata;

import java.util.Optional;

public record StatPathResponse(
        Optional<FileMetadata> metadata
) implements ControlPayload {
    public StatPathResponse {
        metadata = metadata == null ? Optional.empty() : metadata;
    }

    @Override
    public ControlOperation operation() {
        return ControlOperation.STAT_PATH_RESULT;
    }
}
