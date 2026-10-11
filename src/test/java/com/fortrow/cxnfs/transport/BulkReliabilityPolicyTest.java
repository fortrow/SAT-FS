package com.fortrow.cxnfs.transport;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class BulkReliabilityPolicyTest {
    @Test
    void defaultsRelyOnQuicReliabilityAndHashVerification() {
        BulkReliabilityPolicy policy = BulkReliabilityPolicy.defaults();

        assertTrue(policy.quicReliableStreamsRequired());
        assertFalse(policy.udpRetransmissionAssumed());
        assertTrue(policy.authenticatedPacketProtectionRequired());
        assertTrue(policy.lostStreamDataRetransmittedByQuic());
        assertTrue(policy.chunkHashVerificationBeforePublish());
        assertFalse(policy.earlyPayloadPublishAllowed());
    }

    @Test
    void rejectsTreatingUdpAsTheRetransmissionLayer() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new BulkReliabilityPolicy(true, true, true, true, true, false)
        );
    }

    @Test
    void rejectsPublishingEarlyPayloadsBeforeVerification() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new BulkReliabilityPolicy(true, false, true, true, true, true)
        );
    }
}
