package com.fortrow.cxnfs.protocol;

import com.fortrow.cxnfs.manifest.FileManifest;

import java.util.Optional;

public record ManifestResponse(
        Optional<FileManifest> manifest
) implements ControlPayload {
    public ManifestResponse {
        manifest = manifest == null ? Optional.empty() : manifest;
    }

    @Override
    public ControlOperation operation() {
        return ControlOperation.MANIFEST_RESULT;
    }
}
