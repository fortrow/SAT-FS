package com.fortrow.cxnfs.transport;

public record WanTransferSecurityPolicy(
        boolean encryptedTransportRequired,
        boolean plaintextWanPayloadsAllowed,
        boolean authenticatedControlLaneRequired,
        boolean chunkHashVerificationRequired,
        boolean stageUntilMetadataVerified
) {
    public static WanTransferSecurityPolicy defaults() {
        return new WanTransferSecurityPolicy(true, false, true, true, true);
    }

    public WanTransferSecurityPolicy {
        if (!encryptedTransportRequired) {
            throw new IllegalArgumentException("WAN transfers require encrypted transport");
        }
        if (plaintextWanPayloadsAllowed) {
            throw new IllegalArgumentException("WAN transfers must not allow plaintext payloads");
        }
        if (!authenticatedControlLaneRequired) {
            throw new IllegalArgumentException("WAN transfers require an authenticated control lane");
        }
        if (!chunkHashVerificationRequired) {
            throw new IllegalArgumentException("WAN transfers require chunk hash verification");
        }
        if (!stageUntilMetadataVerified) {
            throw new IllegalArgumentException("WAN transfers must stage data until metadata is verified");
        }
    }
}
