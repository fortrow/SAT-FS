package com.fortrow.cxnfs.transport;

public record WriteAdmissionPolicy(
        boolean authenticatedControlSessionRequired,
        boolean authorizedTransferRequired,
        boolean bootstrapMetadataRequiredBeforePayload,
        boolean zeroRttWritePayloadsAllowed,
        boolean unauthenticatedPayloadsBufferedInMemory,
        boolean finalCommitRequiredBeforePublish
) {
    public static WriteAdmissionPolicy defaults() {
        return new WriteAdmissionPolicy(true, true, true, false, false, true);
    }

    public WriteAdmissionPolicy {
        if (!authenticatedControlSessionRequired) {
            throw new IllegalArgumentException("writes require an authenticated control session");
        }
        if (!authorizedTransferRequired) {
            throw new IllegalArgumentException("writes require an authorized transfer");
        }
        if (!bootstrapMetadataRequiredBeforePayload) {
            throw new IllegalArgumentException("write payloads require bootstrap metadata before admission");
        }
        if (zeroRttWritePayloadsAllowed) {
            throw new IllegalArgumentException("0-RTT write payloads are not allowed");
        }
        if (unauthenticatedPayloadsBufferedInMemory) {
            throw new IllegalArgumentException("unauthenticated write payloads must not be buffered in memory");
        }
        if (!finalCommitRequiredBeforePublish) {
            throw new IllegalArgumentException("writes require a final commit before publish");
        }
    }
}
