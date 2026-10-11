package com.fortrow.cxnfs.transport;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class WriteAdmissionPolicyTest {
    @Test
    void defaultsRequireAuthenticatedAdmissionBeforeBuffering() {
        WriteAdmissionPolicy policy = WriteAdmissionPolicy.defaults();

        assertTrue(policy.authenticatedControlSessionRequired());
        assertTrue(policy.authorizedTransferRequired());
        assertTrue(policy.bootstrapMetadataRequiredBeforePayload());
        assertFalse(policy.zeroRttWritePayloadsAllowed());
        assertFalse(policy.unauthenticatedPayloadsBufferedInMemory());
        assertTrue(policy.finalCommitRequiredBeforePublish());
    }

    @Test
    void rejectsUnauthenticatedInMemoryPayloadBuffering() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new WriteAdmissionPolicy(true, true, true, false, true, true)
        );
    }

    @Test
    void rejectsZeroRttWritePayloads() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new WriteAdmissionPolicy(true, true, true, true, false, true)
        );
    }

    @Test
    void defaultsBoundWriteAdmissionLifetimeAndCount() {
        WriteAdmissionLimits limits = WriteAdmissionLimits.defaults();

        assertEquals(Duration.ofMinutes(10), limits.writeTransferTtl());
        assertEquals(4096, limits.maxAdmittedWriteTransfers());
    }

    @Test
    void rejectsInvalidWriteAdmissionLimits() {
        assertThrows(IllegalArgumentException.class, () -> new WriteAdmissionLimits(Duration.ZERO, 1));
        assertThrows(IllegalArgumentException.class, () -> new WriteAdmissionLimits(Duration.ofMinutes(1), 0));
    }
}
