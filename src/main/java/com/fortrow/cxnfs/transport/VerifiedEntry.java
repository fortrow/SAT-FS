package com.fortrow.cxnfs.transport;

import com.fortrow.cxnfs.manifest.FileManifest;

import java.util.UUID;

public record VerifiedEntry(
        UUID transferId,
        int entrySequence,
        FileManifest manifest,
        long verifiedBytes
) {
    public VerifiedEntry {
        if (transferId == null) {
            throw new IllegalArgumentException("transferId is required");
        }
        if (entrySequence < 0) {
            throw new IllegalArgumentException("entrySequence must be non-negative");
        }
        if (manifest == null) {
            throw new IllegalArgumentException("manifest is required");
        }
        if (verifiedBytes < 0) {
            throw new IllegalArgumentException("verifiedBytes must be non-negative");
        }
        if (manifest.size() != verifiedBytes) {
            throw new IllegalArgumentException("verifiedBytes must match manifest size");
        }
    }
}
