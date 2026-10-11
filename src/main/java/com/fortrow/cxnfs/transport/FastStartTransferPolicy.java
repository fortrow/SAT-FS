package com.fortrow.cxnfs.transport;

public record FastStartTransferPolicy(
        boolean enabled,
        boolean sendBootstrapMetadataFirst,
        boolean deferFullMetadataOnControlLane,
        boolean stageEarlyDataUntilVerified,
        boolean finalCommitRequiredForWrites,
        boolean overlapEveryFileChange,
        boolean allowChunkMetadataAfterPayload
) {
    public static FastStartTransferPolicy defaults() {
        return new FastStartTransferPolicy(true, true, true, true, true, true, true);
    }

    public FastStartTransferPolicy {
        if (enabled && !sendBootstrapMetadataFirst) {
            throw new IllegalArgumentException("fast-start transfers require bootstrap metadata before data");
        }
        if (enabled && !deferFullMetadataOnControlLane) {
            throw new IllegalArgumentException("fast-start transfers require deferred control-lane metadata");
        }
        if (enabled && !stageEarlyDataUntilVerified) {
            throw new IllegalArgumentException("fast-start transfers must stage early data until verified");
        }
        if (enabled && !finalCommitRequiredForWrites) {
            throw new IllegalArgumentException("fast-start writes require a final control-lane commit");
        }
        if (enabled && !overlapEveryFileChange) {
            throw new IllegalArgumentException("fast-start transfers must overlap metadata for every file change");
        }
        if (enabled && !allowChunkMetadataAfterPayload) {
            throw new IllegalArgumentException("fast-start transfers must allow chunk metadata to arrive after admitted payloads");
        }
    }
}
