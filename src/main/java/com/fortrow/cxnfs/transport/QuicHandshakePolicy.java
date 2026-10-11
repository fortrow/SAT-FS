package com.fortrow.cxnfs.transport;

public record QuicHandshakePolicy(
        boolean longLivedConnection,
        boolean multiplexCordsOnSingleConnection,
        boolean tlsSessionResumptionEnabled,
        ZeroRttMode zeroRttMode
) {
    public static QuicHandshakePolicy fastStartDefaults() {
        return new QuicHandshakePolicy(true, true, true, ZeroRttMode.READ_ONLY_BOOTSTRAP);
    }

    public QuicHandshakePolicy {
        if (zeroRttMode == null) {
            throw new IllegalArgumentException("zeroRttMode is required");
        }
        if (!longLivedConnection && multiplexCordsOnSingleConnection) {
            throw new IllegalArgumentException("cord multiplexing requires a long-lived QUIC connection");
        }
    }

    public boolean opensHandshakePerCord() {
        return !multiplexCordsOnSingleConnection;
    }

    public boolean allowsZeroRttWrites() {
        return false;
    }
}
