package com.fortrow.cxnfs.transport;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class TransportSecurityConfigTest {
    @Test
    void tcpTlsUsesTheOnlyAllowedCipherSuite() {
        TransportSecurityConfig config = TransportSecurityConfig.tls13ForTcp(Path.of("server.p12"));

        assertEquals(TlsCipherSuite.TLS_AES_128_GCM_SHA256, config.cipherSuite());
        assertEquals("tls_aes_128_gcm_sha256", config.cipherSuite().protocolName());
        assertEquals("TLS_AES_128_GCM_SHA256", config.cipherSuite().javaName());
    }

    @Test
    void quicTlsUsesTheOnlyAllowedCipherSuite() {
        TransportSecurityConfig config = TransportSecurityConfig.quicTls13ForUdp(Path.of("server.p12"));

        assertEquals(TlsCipherSuite.TLS_AES_128_GCM_SHA256, config.cipherSuite());
    }
}
