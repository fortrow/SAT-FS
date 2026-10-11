package com.fortrow.cxnfs.transport;

import java.nio.file.Path;
import java.util.Optional;

public record TransportSecurityConfig(
        SecureTransportMode mode,
        Optional<Path> keyStore,
        Optional<Path> trustStore,
        boolean peerCertificatesRequired,
        TlsCipherSuite cipherSuite
) {
    public static TransportSecurityConfig tls13ForTcp(Path keyStore) {
        return new TransportSecurityConfig(SecureTransportMode.TLS_1_3_TCP, Optional.of(keyStore), Optional.empty(), false);
    }

    public static TransportSecurityConfig mutualTls13ForTcp(Path keyStore, Path trustStore) {
        return new TransportSecurityConfig(SecureTransportMode.TLS_1_3_TCP, Optional.of(keyStore), Optional.of(trustStore), true);
    }

    public static TransportSecurityConfig quicTls13ForUdp(Path keyStore) {
        return new TransportSecurityConfig(SecureTransportMode.QUIC_TLS_1_3_UDP, Optional.of(keyStore), Optional.empty(), false);
    }

    public TransportSecurityConfig(
            SecureTransportMode mode,
            Optional<Path> keyStore,
            Optional<Path> trustStore,
            boolean peerCertificatesRequired
    ) {
        this(mode, keyStore, trustStore, peerCertificatesRequired, TlsCipherSuite.TLS_AES_128_GCM_SHA256);
    }

    public TransportSecurityConfig {
        if (mode == null) {
            throw new IllegalArgumentException("mode is required");
        }
        if (cipherSuite == null) {
            throw new IllegalArgumentException("cipherSuite is required");
        }
        keyStore = keyStore == null ? Optional.empty() : keyStore;
        trustStore = trustStore == null ? Optional.empty() : trustStore;
        if (peerCertificatesRequired && trustStore.isEmpty()) {
            throw new IllegalArgumentException("peer certificate validation requires a trust store");
        }
    }
}
