package com.fortrow.cxnfs.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ControlTlsServerSocketFactoryTest {
    @TempDir
    Path tempDir;

    @Test
    void restrictsControlSocketsToTls13AndTheLockedCipherSuite() throws Exception {
        ControlTlsServerSocketFactory factory = new ControlTlsServerSocketFactory(SSLContext.getDefault());

        try (ServerSocket socket = factory.createServerSocket()) {
            SSLServerSocket sslSocket = assertInstanceOf(SSLServerSocket.class, socket);

            assertArrayEquals(new String[]{"TLSv1.3"}, sslSocket.getEnabledProtocols());
            assertArrayEquals(new String[]{"TLS_AES_128_GCM_SHA256"}, sslSocket.getEnabledCipherSuites());
            assertFalse(sslSocket.getNeedClientAuth());
        }
    }

    @Test
    void loadsTlsContextFromPasswordProtectedKeystore() throws Exception {
        Path keyStorePath = tempDir.resolve("server.p12");
        generateKeyStore(keyStorePath, "secret".toCharArray());

        SSLContext context = new ControlTlsContextFactory().create(
                com.fortrow.cxnfs.transport.TransportSecurityConfig.tls13ForTcp(keyStorePath),
                "secret".toCharArray()
        );

        assertNotNull(context.getServerSocketFactory());
    }

    @Test
    void rejectsKeystoreWithoutPrivateKeyEntry() throws Exception {
        Path keyStorePath = tempDir.resolve("server.p12");
        writeEmptyKeyStore(keyStorePath, "secret".toCharArray());

        java.io.IOException failure = assertThrows(
                java.io.IOException.class,
                () -> new ControlTlsContextFactory().create(
                        com.fortrow.cxnfs.transport.TransportSecurityConfig.tls13ForTcp(keyStorePath),
                        "secret".toCharArray()
                )
        );

        assertNotNull(failure.getMessage());
    }

    @Test
    void rejectsKeystoreWithWrongPassword() throws Exception {
        Path keyStorePath = tempDir.resolve("server.p12");
        writeEmptyKeyStore(keyStorePath, "secret".toCharArray());

        assertThrows(
                java.io.IOException.class,
                () -> new ControlTlsContextFactory().create(
                        com.fortrow.cxnfs.transport.TransportSecurityConfig.tls13ForTcp(keyStorePath),
                        "wrong".toCharArray()
                )
        );
    }

    private static void writeEmptyKeyStore(Path path, char[] password) throws Exception {
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        keyStore.load(null, password);
        try (var output = Files.newOutputStream(path)) {
            keyStore.store(output, password);
        }
    }

    private static void generateKeyStore(Path keyStore, char[] password) throws Exception {
        Process process = new ProcessBuilder(
                "keytool",
                "-genkeypair",
                "-alias",
                "cxnfs",
                "-keyalg",
                "RSA",
                "-keysize",
                "2048",
                "-storetype",
                "PKCS12",
                "-keystore",
                keyStore.toString(),
                "-storepass",
                String.valueOf(password),
                "-keypass",
                String.valueOf(password),
                "-dname",
                "CN=localhost",
                "-validity",
                "7"
        )
                .redirectErrorStream(true)
                .start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (process.waitFor() != 0) {
            throw new IllegalStateException("keytool failed: " + output);
        }
    }
}
