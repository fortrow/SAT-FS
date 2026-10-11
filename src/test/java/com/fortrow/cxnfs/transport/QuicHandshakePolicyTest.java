package com.fortrow.cxnfs.transport;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class QuicHandshakePolicyTest {
    @Test
    void defaultsMultiplexCordsOnOneLongLivedConnection() {
        QuicHandshakePolicy policy = QuicHandshakePolicy.fastStartDefaults();

        assertTrue(policy.longLivedConnection());
        assertTrue(policy.multiplexCordsOnSingleConnection());
        assertTrue(policy.tlsSessionResumptionEnabled());
        assertFalse(policy.opensHandshakePerCord());
        assertFalse(policy.allowsZeroRttWrites());
    }

    @Test
    void rejectsCordMultiplexingWithoutLongLivedConnection() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new QuicHandshakePolicy(false, true, true, ZeroRttMode.READ_ONLY_BOOTSTRAP)
        );
    }
}
