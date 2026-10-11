package com.fortrow.cxnfs.protocol;

import com.fortrow.cxnfs.hash.ContentHash;

public record ManifestRequest(
        ContentHash fileHash
) implements ControlPayload {
    public ManifestRequest {
        if (fileHash == null) {
            throw new IllegalArgumentException("fileHash is required");
        }
    }

    @Override
    public ControlOperation operation() {
        return ControlOperation.GET_MANIFEST;
    }
}
