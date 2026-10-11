package com.fortrow.cxnfs.transport;

public record BulkReliabilityPolicy(
        boolean quicReliableStreamsRequired,
        boolean udpRetransmissionAssumed,
        boolean authenticatedPacketProtectionRequired,
        boolean lostStreamDataRetransmittedByQuic,
        boolean chunkHashVerificationBeforePublish,
        boolean earlyPayloadPublishAllowed
) {
    public static BulkReliabilityPolicy defaults() {
        return new BulkReliabilityPolicy(true, false, true, true, true, false);
    }

    public BulkReliabilityPolicy {
        if (!quicReliableStreamsRequired) {
            throw new IllegalArgumentException("bulk transfer requires QUIC reliable streams");
        }
        if (udpRetransmissionAssumed) {
            throw new IllegalArgumentException("UDP must not be treated as a retransmission protocol");
        }
        if (!authenticatedPacketProtectionRequired) {
            throw new IllegalArgumentException("bulk transfer requires authenticated QUIC packet protection");
        }
        if (!lostStreamDataRetransmittedByQuic) {
            throw new IllegalArgumentException("bulk transfer requires QUIC retransmission of lost stream data");
        }
        if (!chunkHashVerificationBeforePublish) {
            throw new IllegalArgumentException("bulk transfer requires chunk hash verification before publish");
        }
        if (earlyPayloadPublishAllowed) {
            throw new IllegalArgumentException("early payloads must not publish before verification");
        }
    }
}
