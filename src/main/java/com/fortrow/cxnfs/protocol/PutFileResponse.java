package com.fortrow.cxnfs.protocol;

import com.fortrow.cxnfs.hash.ContentHash;
import com.fortrow.cxnfs.manifest.FileManifest;

import java.util.List;

public record PutFileResponse(
        FileManifest manifest,
        List<ContentHash> missingChunks
) implements ControlPayload {
    public PutFileResponse {
        if (manifest == null) {
            throw new IllegalArgumentException("manifest is required");
        }
        missingChunks = missingChunks == null ? List.of() : List.copyOf(missingChunks);
    }

    @Override
    public ControlOperation operation() {
        return ControlOperation.PUT_FILE_RESULT;
    }
}
