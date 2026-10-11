package com.fortrow.cxnfs.transport;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class WanTransferSecurityPolicyTest {
    @Test
    void defaultsForbidPlaintextWanPayloads() {
        WanTransferSecurityPolicy policy = WanTransferSecurityPolicy.defaults();

        assertTrue(policy.encryptedTransportRequired());
        assertFalse(policy.plaintextWanPayloadsAllowed());
        assertTrue(policy.authenticatedControlLaneRequired());
        assertTrue(policy.chunkHashVerificationRequired());
        assertTrue(policy.stageUntilMetadataVerified());
    }

    @Test
    void rejectsPlaintextWanPayloads() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new WanTransferSecurityPolicy(true, true, true, true, true)
        );
    }
}
