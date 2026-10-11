package com.fortrow.cxnfs.client;

import com.fortrow.cxnfs.transport.TlsCipherSuite;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLSocket;
import java.net.Socket;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

final class ControlTlsClientSocketFactoryTest {
    @Test
    void configuresTls13LockedCipherSuiteAndEndpointIdentification() throws Exception {
        ControlTlsClientSocketFactory factory = new ControlTlsClientSocketFactory(
                new ClientTlsContextFactory().createDefault()
        );

        try (Socket socket = factory.createSocket()) {
            SSLSocket sslSocket = assertInstanceOf(SSLSocket.class, socket);

            assertArrayEquals(new String[]{"TLSv1.3"}, sslSocket.getEnabledProtocols());
            assertArrayEquals(
                    new String[]{TlsCipherSuite.TLS_AES_128_GCM_SHA256.javaName()},
                    sslSocket.getEnabledCipherSuites()
            );
            assertEquals("HTTPS", sslSocket.getSSLParameters().getEndpointIdentificationAlgorithm());
        }
    }
}
